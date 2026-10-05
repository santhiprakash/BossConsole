package ai.rever.boss.components.workspaces

import ai.rever.boss.plugin.ui.BossThemeController
import ai.rever.boss.plugin.ui.BossThemes
import ai.rever.boss.utils.logging.BossLogger
import ai.rever.boss.utils.logging.LogCategory
import ai.rever.boss.utils.logging.decodeFailure
import androidx.compose.ui.graphics.Color
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.SerializationException
import kotlin.time.Clock

/**
 * Manages layout workspaces with file-based storage.
 *
 * `fileManager` is a test seam: pass a manager pointed at a directory the test controls.
 * The app passes nothing, which resolves against the user's real documents location.
 */
class WorkspaceManager(
    private val fileManager: WorkspaceFileManager = WorkspaceFileManager(),
) {
    private val logger = BossLogger.forComponent("WorkspaceManager")
    private val _currentWorkspace = MutableStateFlow<LayoutWorkspace?>(null)
    val currentWorkspace: StateFlow<LayoutWorkspace?> = _currentWorkspace.asStateFlow()

    private val _workspaces = MutableStateFlow<List<LayoutWorkspace>>(emptyList())
    val workspaces: StateFlow<List<LayoutWorkspace>> = _workspaces.asStateFlow()

    /**
     * Every workspace each open window is RUNNING, by window id.
     *
     * A set per window, not a single id, because a window runs several workspaces at once.
     * Switching does not tear the old one down - `SplitViewState.preserveCurrentState` keeps its
     * whole split tree, live tab components and all - so a workspace you have switched away from
     * is still going, and its tabs still appear in `collectAllTabs`.
     *
     * [currentWorkspace] answers none of this. It is one value on a manager every window shares:
     * with two windows on different workspaces it names whichever loaded last, and it never knew
     * about the ones running behind the one on screen.
     */
    private val _windowWorkspaces = MutableStateFlow<Map<String, Set<String>>>(emptyMap())
    val windowWorkspaces: StateFlow<Map<String, Set<String>>> = _windowWorkspaces.asStateFlow()

    /** Every workspace running anywhere, across every open window. */
    val liveWorkspaceIds: Set<String> get() =
        _windowWorkspaces.value.values
            .flatten()
            .toSet()

    /**
     * Record every workspace [windowId] is running.
     *
     * Called by the window itself, which is the only thing that knows - the live set lives in
     * that window's SplitViewState. There is no single place inside this manager to hook:
     * `loadWorkspace` is reached from restore, from menu actions, from plugins and from the
     * workspace button, and only some of those know which window they are acting for.
     */
    fun setWindowWorkspaces(
        windowId: String,
        workspaceIds: Set<String>,
    ) {
        _windowWorkspaces.value =
            if (workspaceIds.isEmpty()) {
                _windowWorkspaces.value - windowId
            } else {
                _windowWorkspaces.value + (windowId to workspaceIds)
            }
    }

    /** Forget a window, on close. Without this its workspace stays marked active for ever. */
    fun releaseWindow(windowId: String) {
        _windowWorkspaces.value = _windowWorkspaces.value - windowId
        // And its unsaved marks, for the same reason: a closed window's dirt is not anybody's to
        // save, and a reused window id would inherit it.
        _unsavedWorkspaces.value = _unsavedWorkspaces.value - windowId
    }

    /**
     * Ids of the Spaces whose live layout differs from what is on disk, by window.
     *
     * **Per window, and that is not optional.** Two windows run different Spaces and each one's
     * layout is its own; one flat set would light the Save button in a window that has nothing to
     * save the moment the other window was edited. The window is the only party that can answer at
     * all - the live layout lives in its `SplitViewState` - so it reports, exactly as
     * [setWindowWorkspaces] has it report which Spaces it is running.
     *
     * A `StateFlow`, so the vertical bar's Save affordance can watch it. This replaces
     * `TabTreeState.modifiedWorkspaces`, which was written from three places and READ FROM NONE -
     * there was no dirty state in the app, only the bookkeeping for one.
     */
    private val _unsavedWorkspaces = MutableStateFlow<Map<String, Set<String>>>(emptyMap())

    /** Every Space with unsaved changes, in every window. See [isWorkspaceUnsavedIn]. */
    val unsavedWorkspaces: StateFlow<Map<String, Set<String>>> = _unsavedWorkspaces.asStateFlow()

    /**
     * Record whether [windowId]'s copy of [workspaceId] has changes that are not on disk.
     *
     * Called from the window's own layout watcher, which extracts the live layout already. The
     * decision itself is [isUnsaved], which is pure and tested; this only stores it.
     */
    fun setWorkspaceUnsaved(
        windowId: String,
        workspaceId: String,
        unsaved: Boolean,
    ) {
        val current = _unsavedWorkspaces.value[windowId].orEmpty()
        val updated = if (unsaved) current + workspaceId else current - workspaceId
        if (updated == current) return
        _unsavedWorkspaces.value =
            if (updated.isEmpty()) {
                _unsavedWorkspaces.value - windowId
            } else {
                _unsavedWorkspaces.value + (windowId to updated)
            }
    }

    /** Whether [windowId] holds unsaved changes to [workspaceId]. */
    fun isWorkspaceUnsavedIn(
        windowId: String,
        workspaceId: String,
    ): Boolean = workspaceId in _unsavedWorkspaces.value[windowId].orEmpty()

    /**
     * The Space with [workspaceId] as it exists ON DISK, or null if nothing there answers to it.
     *
     * [workspaces] is the honest answer to that question and [currentWorkspace] is not:
     * `updateCurrentWorkspace` writes the live layout into it BEFORE the save is attempted, so
     * comparing against it would read clean the instant an auto-save was queued rather than when
     * the bytes landed. This list is only replaced once `fileManager` has returned a path.
     */
    fun savedCopyOf(workspaceId: String): LayoutWorkspace? = _workspaces.value.firstOrNull { it.id == workspaceId }

    private val scope = CoroutineScope(Dispatchers.Main + SupervisorJob())

    /** Picker list; recovery records remain available internally even when the slot is hidden. */
    val visibleWorkspaces: StateFlow<List<LayoutWorkspace>> =
        combine(workspaces, WorkspaceSettingsManager.currentSettings) { spaces, settings ->
            visibleSessionSpaces(spaces, settings.enableLastSessionSpace)
        }.stateIn(scope, SharingStarted.Eagerly, emptyList())

    /**
     * The file each Space was LOADED from, by id, for the ones whose path predates
     * [WorkspaceFileManagerCommon.fileNameForId].
     *
     * A new Space is written to `<id>.json` and needs no entry. A Space read out of
     * `Code_Review.json` keeps saving into `Code_Review.json`, so **an upgrade rewrites and renames
     * nothing on disk** - the same in-memory-migration discipline `mergeSavedWorkspaces` uses, and
     * for the same reason: a launch that half-wrote would be worse than any naming.
     *
     * Populated by the load scan, which already reads every file's id out of its contents.
     */
    private val loadedFileNames = mutableMapOf<String, String>()

    /** Where [workspace] should be written: the file it came from, or `<id>.json`. */
    private fun fileNameFor(workspace: LayoutWorkspace): String =
        loadedFileNames[workspace.id] ?: WorkspaceFileManagerCommon.fileNameForId(workspace.id)

    /**
     * Where [workspace] may be WRITTEN - or deleted through: [fileNameFor]'s answer, or null
     * when that answer is a reserved store record this writer must not touch (#1820).
     *
     * The gate lives here, at the one place every write target is resolved, because the
     * reserved name can arrive two ways and an id-level check sees only one of them: an id
     * that itself resolves onto a record (`Space_Themes` -> `Space_Themes.json`), AND a
     * `loadedFileNames` binding - a file on disk NAMED like a record can hold contents that
     * parse as a Space carrying an ordinary id (only the document records are scan-skipped;
     * a corrupt or hand-edited `Last_Session.json` still loads), and the scan used to record
     * that name under the foreign id. Without this, `saveCurrentWorkspace` rewrote the
     * session record, `renameWorkspaceById` rewrote it, and `deleteWorkspaceById` deleted it.
     *
     * One id is allowed through: [LAST_SESSION_ID], the record's own. The live record writes
     * `last-session.json`, and one loaded off a pre-[WorkspaceFileManagerCommon.fileNameForId]
     * install keeps writing `Last_Session.json` through its `loadedFileNames` binding - both
     * are session-record spellings, not document records. The document records
     * (`Space_Themes.json`, `Last_Session_Set.json`) have no legitimate writer on this path at
     * all: they are written through `writeDocumentBlocking`, and they are refused here even
     * for the session id.
     */
    private fun writeTargetFor(workspace: LayoutWorkspace): String? =
        fileNameFor(workspace).takeUnless { isReservedTargetForOtherId(it, workspace.id) }

    /**
     * Whether the resolved [fileName] is a reserved store record [workspaceId] must not write:
     * every reserved name for a foreign id, and the DOCUMENT records even for [LAST_SESSION_ID] -
     * the record's own writer only ever lands on a session-record spelling. Shared by the scan
     * (which must not create such a binding) and [writeTargetFor] (which must not honour one),
     * so the two cannot drift.
     */
    private fun isReservedTargetForOtherId(
        fileName: String,
        workspaceId: String,
    ): Boolean {
        reservedWorkspaceStoreFileName(fileName) ?: return false
        return !workspaceId.equals(LAST_SESSION_ID, ignoreCase = true) ||
            WorkspaceFileManagerCommon.isReservedDocumentFileName(fileName)
    }

    // Callback for when a workspace is deleted
    private var onWorkspaceDeleted: ((String) -> Unit)? = null

    /**
     * Which theme each Space has been given, by workspace id - the OVERRIDES only.
     *
     * A Space that names none is not absent from the feature, it is riding a default:
     * [TEMPLATE_SPACE_THEMES] for one of the eight layouts BOSS ships, and the Settings choice for
     * everything else. [spaceThemeId] is what resolves the three; nothing should read this map
     * without it.
     */
    private val _spaceThemes = MutableStateFlow<Map<String, String>>(emptyMap())
    val spaceThemes: StateFlow<Map<String, String>> = _spaceThemes.asStateFlow()

    /**
     * Whether anything has assigned a Space theme since this manager was built.
     *
     * The disk read is a SEED, and a seed must not land on top of a decision. [loadSpaceThemes] is
     * asynchronous, so a `setSpaceTheme` made while it is still in flight - the template
     * inheritance in `WorkspaceTemplate` does exactly that, in the first moments of a manager's
     * life - was overwritten by whatever the file happened to say, silently and only sometimes.
     * Merging the two instead would fix a set and break a CLEAR, since a cleared key is absent
     * from both maps and the file would put it back.
     */
    private var spaceThemesAssigned = false

    /**
     * The colour each Space is wearing, by workspace id, for `ActiveTabsProvider.workspaceAccents`.
     *
     * Keyed over every Space the app knows - the eight shipped layouts and everything saved
     * ([workspaces] already holds both), plus whatever is running and whatever has an override -
     * because the panels that read this are listing Spaces they are not in. The union is taken
     * rather than [workspaces] alone so a Space running from a file that has since been deleted
     * still has a colour while it is on screen.
     *
     * Derived from all three inputs, so it moves when a Space is re-themed AND when the Settings
     * baseline changes: every Space with no theme of its own is riding that baseline, so a Settings
     * pick has to repaint all of them.
     */
    val spaceAccents: StateFlow<Map<String, Color>> =
        combine(
            _workspaces,
            _windowWorkspaces,
            _spaceThemes,
            SettingsThemeBaseline.themeId,
        ) { spaces, windows, overrides, baseline ->
            (spaces.map { it.id } + windows.values.flatten() + overrides.keys)
                .filter { it.isNotEmpty() }
                .distinct()
                .associateWith { BossThemes.byId(spaceThemeId(it, overrides, baseline)).colors.signal }
        }.stateIn(scope, SharingStarted.Eagerly, emptyMap())

    /** The theme [workspaceId] should be showing, resolved through [spaceThemeId]. */
    fun themeIdFor(workspaceId: String): String {
        val overrides = _spaceThemes.value
        return spaceThemeId(workspaceId, overrides, SettingsThemeBaseline.themeId.value)
    }

    /**
     * Give [workspaceId] a theme of its own, or hand it back to its default when [themeId] is null.
     *
     * **`BossThemeController.select` and nothing else.** `AppThemeSettingsManager.select` would
     * write the id into `app-theme-settings.json`, and after two Space switches the baseline the
     * user chose in Settings would be gone - along with the answer for every Space that names no
     * theme. A Space theme is an override LAYERED OVER that choice, which is why the two writes
     * land in different files.
     */
    fun setSpaceTheme(
        workspaceId: String,
        themeId: String?,
    ) {
        val updated = withSpaceTheme(_spaceThemes.value, workspaceId, themeId, SettingsThemeBaseline.themeId.value)
        // Marked even when nothing moves, because "the answer is already what you asked for" is
        // still a decision the seed must not overwrite.
        spaceThemesAssigned = true
        if (updated == _spaceThemes.value) return
        _spaceThemes.value = updated
        // Only when it is the Space on screen: re-theming a Space you are not in must not re-skin
        // the app out from under you.
        if (_currentWorkspace.value?.id == workspaceId) applySpaceTheme(workspaceId)
        scope.launch {
            val written =
                withContext(Dispatchers.IO) {
                    fileManager.writeDocumentBlocking(SPACE_THEMES_FILE, spaceThemesDocument(updated))
                }
            if (!written) {
                logger.warn(
                    LogCategory.WORKSPACE,
                    "Space theme write failed",
                    mapOf("workspace" to workspaceId, "themes" to updated.size.toString()),
                )
            }
        }
    }

    /** Put the app on [workspaceId]'s theme. A no-op for an id whose answer is already showing. */
    private fun applySpaceTheme(workspaceId: String) {
        BossThemeController.select(themeIdFor(workspaceId))
    }

    private fun loadSpaceThemes() {
        scope.launch {
            val json = fileManager.loadDocument(SPACE_THEMES_FILE)
            val assignments = spaceThemesFrom(json)
            if (json != null && assignments.isEmpty()) {
                // There were bytes and they said nothing: a broken record, or one naming only
                // themes this build has retired. An empty map is never written (see
                // `spaceThemesDocument`), so there is no innocent case this shouts about.
                logger.warn(LogCategory.WORKSPACE, "Space themes record could not be read")
            }
            // The seed lands only on a store nobody has written to yet. See [spaceThemesAssigned].
            if (!spaceThemesAssigned) _spaceThemes.value = assignments
            // Re-resolve whatever is already on screen. This read is asynchronous and the session
            // restore does not wait for it, so a Space entered first would be sitting on the
            // Settings theme rather than its own until the next switch.
            _currentWorkspace.value?.let { applySpaceTheme(it.id) }
        }
    }

    init {
        // Load workspaces from both predefined and saved files
        loadAllWorkspaces()
        loadSpaceThemes()
    }

    private fun loadAllWorkspaces() {
        scope.launch {
            val saved = mutableListOf<LayoutWorkspace>()

            // Load saved workspaces from disk
            try {
                val savedWorkspaces =
                    withContext(Dispatchers.IO) {
                        fileManager.listWorkspaces()
                    }
                savedWorkspaces.forEach { fileInfo ->
                    // The reserved record files live in this directory and are not Spaces - the
                    // session-set store and the Space-theme store. The scan is "every *.json",
                    // so without this each is deserialized as a Space on every launch, fails,
                    // and logs a warning for ever. The list lives in WorkspaceFileManagerCommon,
                    // shared with the write-side guard, which extends it with the session-record
                    // spellings it must refuse too, so the two cannot drift apart (#926).
                    if (WorkspaceFileManagerCommon.isReservedDocumentFileName(fileInfo.fileName)) return@forEach
                    adoptScannedWorkspace(fileInfo, saved)
                }
            } catch (e: Exception) {
                // Log error but continue with predefined workspaces
                logger.warn(LogCategory.WORKSPACE, "Error loading workspaces", error = e)
            }

            // Merged by ID, never by name. A saved file called "Codex" used to be DROPPED here in
            // favour of the shipped layout of that name, and so did a save made while the current
            // Space was a built-in - which is the Save button not working. See
            // `mergeSavedWorkspaces`, which also says what becomes of a legacy file whose id IS a
            // built-in's.
            _workspaces.value = mergeSavedWorkspaces(PredefinedWorkspaces.allWorkspaces, saved)
        }
    }

    /**
     * Adopt one file the directory scan found into [saved]: read it, give it a stable id, and
     * remember the file it came from - or refuse that binding when the file is a reserved store
     * record carrying another id (#1820).
     */
    private suspend fun adoptScannedWorkspace(
        fileInfo: WorkspaceFileInfo,
        saved: MutableList<LayoutWorkspace>,
    ) {
        val workspace =
            withContext(Dispatchers.IO) {
                fileManager.loadWorkspace(fileInfo.fileName)
            } ?: return
        // Ensure workspace has an ID
        val withId = workspace.withStableId()
        if (fileInfo.fileName == ".json") {
            adoptNamelessWorkspace(withId, saved)
            return
        }
        // A file NAMED like a store record but carrying another id is not the record. Only the
        // document records are skipped by the caller, so a corrupt or hand-edited
        // `Last_Session.json` / `last-session.json` still loads as a Space - which is fine, but
        // it must not keep the name: the binding and the id write-back below would both put a
        // foreign id's writes onto the record file. The real record is exempt - it carries
        // [LAST_SESSION_ID] legitimately, and a legacy `Last_Session.json` stays its file.
        val reservedNameForOtherId = isReservedTargetForOtherId(fileInfo.fileName, withId.id)
        if (withId !== workspace) {
            // A file carrying no id minted a fresh one on EVERY launch, so the
            // Space's identity - session-set membership, preserved-state keys -
            // never matched across a relaunch. Write the minted id back into the
            // file it came from, once, and the next load reads a stable id. The
            // serializer re-emits the model, so a hand-maintained file loses its
            // formatting and unknown keys: BOSS takes ownership of the file on
            // first launch. If the write fails the next launch would mint yet
            // another id - the original bug, silently - so say so.
            //
            // A reserved name under a foreign id is refused the write entirely:
            // persisting the minted id into `Last_Session.json` is a write to the
            // record itself (#1820), on a case-insensitive filesystem the very
            // file the live record writes. The Space loads regardless and mints
            // again next launch - the lesser cost.
            if (reservedNameForOtherId) {
                logger.warn(
                    LogCategory.WORKSPACE,
                    "Refusing to persist a minted workspace id into the reserved store file",
                    mapOf("fileName" to fileInfo.fileName),
                )
            } else if (fileManager.saveWorkspace(withId, fileInfo.fileName) == null) {
                logger.warn(
                    LogCategory.WORKSPACE,
                    "Could not persist a minted workspace id - it will be re-minted next launch",
                    mapOf("fileName" to fileInfo.fileName),
                )
            }
        }
        saved.add(withId)
        // Remember the file it came from, so a legacy path keeps being this
        // Space's file. A file already named `<id>.json` records the same answer
        // `fileNameFor` would derive, so the entry is harmless either way.
        //
        // A reserved name under a foreign id is the one harmful entry: it is how
        // this Space's save/rename/delete used to reach the store record, and it
        // is the state `writeTargetFor` now refuses (#1820).
        if (!reservedNameForOtherId) {
            loadedFileNames[withId.id] = fileInfo.fileName
        }
    }

    /**
     * Adopt the legacy nameless workspace file: a pre-id build wrote the literal `.json`, and
     * that file IS a real Space (the last id-less import, the one that survived the
     * overwrite). Save it under its own `<id>.json` and remove the nameless file so it is not
     * re-adopted every launch. Never write it back AS `.json`.
     *
     * The one refusal: a nameless file can still carry an id whose own `fileNameForId` lands
     * on a store record - `{"id":"Space_Themes"}` - and adopting it would write the record
     * (#1820). Load it unbound instead: the write gate refuses its saves, and the next launch
     * refuses the adoption again.
     */
    private suspend fun adoptNamelessWorkspace(
        withId: LayoutWorkspace,
        saved: MutableList<LayoutWorkspace>,
    ) {
        val adoptedName = WorkspaceFileManagerCommon.fileNameForId(withId.id)
        if (isReservedTargetForOtherId(adoptedName, withId.id)) {
            logger.warn(
                LogCategory.WORKSPACE,
                "Refusing to adopt the nameless workspace file onto the reserved store file '$adoptedName'",
                mapOf("workspaceId" to withId.id),
            )
            saved.add(withId)
            return
        }
        if (fileManager.saveWorkspace(withId, adoptedName) != null) {
            runCatching { fileManager.deleteWorkspace(".json") }
        } else {
            logger.warn(
                LogCategory.WORKSPACE,
                "Could not adopt a legacy nameless workspace file - it will be retried next launch",
            )
        }
        saved.add(withId)
        loadedFileNames[withId.id] = adoptedName
    }

    /**
     * Load a workspace - ENTER this Space, which is also what re-skins the app.
     *
     * **The one door.** Every way into a Space goes through here: the Space button and its menu,
     * the picker, the switch path, a deep link, the CLI, a plugin's `WorkspaceDataProvider`, the
     * fresh-start default and both session restores. Hanging the theme off this rather than off a
     * collector on [currentWorkspace] is what keeps a rename or a save - which also write that
     * flow - from being read as entering somewhere.
     *
     * With two windows on different Spaces the last switch wins, because there is one
     * `BossThemeController` for the process and one app to skin. That is accepted, and it is the
     * same thing [currentWorkspace] itself has always done.
     */
    fun loadWorkspace(workspace: LayoutWorkspace) {
        _currentWorkspace.value = workspace
        applySpaceTheme(workspace.id)
    }

    /**
     * Save current workspace to disk
     */
    fun saveCurrentWorkspace(name: String? = null): LayoutWorkspace? = saveCurrentWorkspace(name, onSaved = {})

    internal fun saveCurrentWorkspace(
        name: String?,
        onSaved: (LayoutWorkspace) -> Unit,
        onFailed: (String) -> Unit = {},
    ): LayoutWorkspace? {
        val current = _currentWorkspace.value ?: return null
        val now = Clock.System.now().toEpochMilliseconds()
        val savedWorkspace =
            if (isSpaceSlot(current.id)) {
                // Saving while the current Space is a SLOT - a shipped layout, or the `last-session`
                // autosave record - creates the user's own copy rather than writing over the slot:
                // a new id, and a name that collides with nothing and is never "Last Session".
                // Writing the slot's own id and name is what the old behaviour did, and the file it
                // produced was silently dropped on the next launch. See `savedCopyOfSlot`.
                savedCopyOfSlot(
                    current = current,
                    id = LayoutWorkspace.generateId(),
                    now = now,
                    // Only other SPACES, so saving out of the shipped Claude Code gives a Space
                    // called "Claude Code" rather than "Claude Code 2". See `savedSpaceNames`.
                    takenNames = savedSpaceNames(_workspaces.value),
                    requestedName = name,
                )
            } else {
                current.copy(
                    id = current.id.ifBlank { mintWorkspaceId() },
                    // A typed name goes through `uniqueWorkspaceName` too, which it used to
                    // bypass entirely - the one path that could still put two identical rows in
                    // the list. Its own name is never "taken" by itself, so re-saving a Space
                    // under the name it already has is not numbered.
                    name =
                        name?.let {
                            uniqueWorkspaceName(it, savedSpaceNames(_workspaces.value) - current.name)
                        } ?: current.name,
                    timestamp = now,
                )
            }

        writeSavedWorkspace(savedWorkspace, onSaved, onFailed)

        return savedWorkspace
    }

    /**
     * Write [savedWorkspace] to disk on the manager's scope and, once the bytes landed, commit
     * it to the list and fire [onSaved]. [onFailed] hears about BOTH failure answers: a write
     * the store refused through the reserved-name gate, and a write the filesystem refused.
     */
    private fun writeSavedWorkspace(
        savedWorkspace: LayoutWorkspace,
        onSaved: (LayoutWorkspace) -> Unit,
        onFailed: (String) -> Unit,
    ) {
        scope.launch {
            // Save to disk (on IO thread), through the reserved-store gate: a Space whose
            // resolved target is a store record is refused, not written over it (#1820).
            val fileName = writeTargetFor(savedWorkspace)
            if (fileName == null) {
                logger.warn(
                    LogCategory.WORKSPACE,
                    "Refusing to save a Space over the reserved store file '${fileNameFor(savedWorkspace)}'",
                    mapOf("workspace" to savedWorkspace.name, "id" to savedWorkspace.id),
                )
                onFailed(savedWorkspace.name)
                return@launch
            }
            val filePath =
                withContext(Dispatchers.IO) {
                    fileManager.saveWorkspace(savedWorkspace, fileName)
                }
            if (filePath != null) {
                loadedFileNames[savedWorkspace.id] = fileName
                // Update workspaces list (on Main thread), keyed by ID for the reason
                // `mergeSavedWorkspaces` is: by NAME, saving a Space of the user's that happens to
                // be called "Codex" replaced the SHIPPED Codex in this list, so the Templates
                // section lost a tile for the rest of the session. By name a RENAME also appended a
                // second entry rather than updating the one it renamed, since the new name matched
                // nothing.
                val workspaces = _workspaces.value.toMutableList()
                val existingIndex = workspaces.indexOfFirst { it.id == savedWorkspace.id }

                if (existingIndex >= 0) {
                    workspaces[existingIndex] = savedWorkspace
                } else {
                    workspaces.add(savedWorkspace)
                }

                _workspaces.value = workspaces
                _currentWorkspace.value = savedWorkspace
                onSaved(savedWorkspace)
            } else {
                // `fileManager` already logged the write error; the caller must still hear the
                // save FAILED, because the unsaved mark is derived from the list above - without
                // a message the user gets a Space button that stays lit and no explanation.
                logger.warn(
                    LogCategory.WORKSPACE,
                    "Space save did not land on disk; reporting the failure to the caller",
                    mapOf("workspace" to savedWorkspace.name, "id" to savedWorkspace.id),
                )
                onFailed(savedWorkspace.name)
            }
        }
    }

    /**
     * Write [record] as the Last Session file on the calling thread, and return whether it landed.
     * [noteLastSessionRecordWritten] then refreshes the list entry, on the manager's dispatcher,
     * unless the caller was cancelled meanwhile: the window is going away then, and the file is
     * already on disk.
     *
     * The layout watcher's record write, blocking so it can sit beside the set's inside
     * `LastSessionCoordinator.writeInSession`, under the lock the shutdown write takes (see
     * `writeInSessionRecovery`). Deliberately does NOT
     * touch [currentWorkspace]: while the user is working in a named Space that is the Space they
     * are in, and stamping it "Last Session" would rename it under them. The caller sets it, from
     * `LayoutWatcherWrite.current`, which is the live layout under the identity it already has.
     */
    fun writeLastSessionRecordBlocking(record: LayoutWorkspace): Boolean {
        // The one id `writeTargetFor` lets onto a reserved name: this IS the record's writer.
        val fileName = writeTargetFor(record)
        if (fileName != null && fileManager.saveWorkspaceBlocking(record, fileName) != null) return true
        logger.warn(LogCategory.WORKSPACE, "Last Session record write failed")
        return false
    }

    /**
     * Refresh the list entry for a [record] [writeLastSessionRecordBlocking] has just written.
     *
     * The list entry IS refreshed, because [savedCopyOf] reads that list to answer "what is on
     * disk" and the unsaved flag is derived from the answer - an entry left stale would say the
     * Last Session record needs saving when it had just been written.
     */
    fun noteLastSessionRecordWritten(record: LayoutWorkspace) {
        loadedFileNames[record.id] = fileNameFor(record)
        _workspaces.value =
            _workspaces.value.toMutableList().also { workspaces ->
                // By ID. By NAME this wrote over whatever row happened to be called "Last
                // Session", which after the merge stopped keying on names can be a Space of the
                // user's that is merely CALLED that.
                val existingIndex = workspaces.indexOfFirst { it.id == record.id }
                if (existingIndex >= 0) workspaces[existingIndex] = record else workspaces.add(record)
            }
    }

    /**
     * Persist [layout] as the "Last Session" workspace, blocking until the file
     * has been written, and return whether the write succeeded.
     *
     * For the shutdown path only. [saveCurrentWorkspace] is fire-and-forget on a
     * `Dispatchers.Main` scope, which cannot be trusted while the app is closing:
     * the coroutine is queued behind the teardown that scheduled it, so the
     * process can exit before the write ever starts — the layout users report as
     * "disappeared". This writes on the calling thread instead, so returning
     * means the session is on disk.
     */
    fun saveLastSessionBlocking(layout: LayoutWorkspace): Boolean {
        val lastSession =
            asLastSession(layout).copy(
                timestamp = Clock.System.now().toEpochMilliseconds(),
            )
        val fileName = writeTargetFor(lastSession)
        val filePath = fileName?.let { fileManager.saveWorkspaceBlocking(lastSession, it) }
        if (filePath == null) {
            logger.warn(LogCategory.WORKSPACE, "Last Session save failed", mapOf("workspace" to lastSession.name))
            return false
        }
        loadedFileNames[lastSession.id] = fileName
        _currentWorkspace.value = lastSession
        _workspaces.value =
            _workspaces.value.toMutableList().also { workspaces ->
                // By ID, for the reason `noteLastSessionRecordWritten` states.
                val existingIndex = workspaces.indexOfFirst { it.id == lastSession.id }
                if (existingIndex >= 0) workspaces[existingIndex] = lastSession else workspaces.add(lastSession)
            }
        return true
    }

    /**
     * Persist [set] as the multi-Space session record, blocking until the bytes are down, and
     * return whether the write succeeded. A null [set] DELETES the record.
     *
     * The shutdown path's other half, beside [saveLastSessionBlocking], and blocking for the same
     * reason: a coroutine queued on `Dispatchers.Main` while the app is closing may never run.
     * Both are called under `LastSessionCoordinator`'s single claim, so the two files are written
     * together by one window and cannot disagree about which session they describe. During the
     * session the same window keeps both current, with this and [writeLastSessionRecordBlocking]
     * under `LastSessionCoordinator.writeInSession`, so a hard kill finds a set as fresh as the
     * record rather than the one the previous clean shutdown left.
     *
     * The delete is not tidiness. Restore reads the set in preference to `Last_Session.json`, so a
     * set left behind by a three-Space session would reopen two Spaces after a session that had
     * closed them.
     */
    fun saveLastSessionSetBlocking(set: LastSessionSet?): Boolean {
        val written =
            fileManager.writeDocumentBlocking(
                LAST_SESSION_SET_FILE,
                set?.let { LastSessionSetSerializer.serialize(it) },
            )
        if (!written) {
            logger.warn(
                LogCategory.WORKSPACE,
                "Last Session set write failed",
                mapOf("spaces" to (set?.spaces?.size ?: 0).toString(), "removing" to (set == null).toString()),
            )
        }
        return written
    }

    /**
     * The multi-Space session record on disk, or null when there is none or it cannot be read.
     *
     * Null is the ordinary answer, not an error: an installed build upgrading into this has only
     * `Last_Session.json`. New builds write a set even for a single Space to retain its id. A file that
     * cannot be parsed is also null, so a truncated or hand-broken record falls back to the
     * single-Space restore rather than failing the launch.
     */
    suspend fun loadLastSessionSet(): LastSessionSet? {
        val json = fileManager.loadDocument(LAST_SESSION_SET_FILE) ?: return null
        return try {
            LastSessionSetSerializer.deserialize(json)
        } catch (e: SerializationException) {
            // Saved session documents can contain tab URLs, project paths and terminal commands.
            // kotlinx includes the input document in its decoder message, so keep only location
            // and exception-type diagnostics.
            logger.warn(LogCategory.WORKSPACE, "Last Session set could not be read", decodeFailure(e))
            null
        } catch (e: Exception) {
            logger.warn(LogCategory.WORKSPACE, "Last Session set could not be read", error = e)
            null
        }
    }

    /**
     * Reset to default workspace
     */
    fun resetToDefault() {
        _currentWorkspace.value = null
    }

    /**
     * Export workspace to JSON
     */
    fun exportWorkspace(workspace: LayoutWorkspace): String = WorkspaceSerializer.serialize(workspace)

    /**
     * Import workspace from JSON
     */
    fun importWorkspace(jsonString: String): LayoutWorkspace? =
        try {
            // A hand-written or agent-authored file commonly carries no id. Blank used to save
            // as the literal file ".json", which every id-less import then shared - the second
            // destroyed the first. The minted id is written back with the Space below, so it
            // stays stable across launches instead of being re-minted on every load.
            val raw = WorkspaceSerializer.deserialize(jsonString)
            val withSafeId = raw.withImportableId()
            if (raw.id != withSafeId.id) {
                logger.info(
                    LogCategory.WORKSPACE,
                    "Re-minted imported workspace id to avoid collisions or reserved record overwrite",
                    mapOf("previousId" to raw.id, "newId" to withSafeId.id),
                )
            }
            val uniqueName = uniqueWorkspaceName(withSafeId.name, savedSpaceNames(_workspaces.value))
            val workspace = if (uniqueName != withSafeId.name) withSafeId.copy(name = uniqueName) else withSafeId

            // The reserved-store gate the MCP create path applies to a caller-chosen id (#926,
            // #964), evaluated on the name the save will write (#1627) through the helper every
            // writer now resolves the target with (#1820). withImportableId re-mints an id whose
            // own fileNameForId lands on a reserved record, but fileNameFor can instead answer a
            // name the load scan recorded into loadedFileNames - a name the identity rule never
            // consulted - so the check runs on the actual write target. Refusal mirrors the MCP
            // answer: before anything is created or applied, so the import reports failure whole
            // rather than registering a Space that never persisted.
            val fileName = writeTargetFor(workspace)
            if (fileName == null) {
                logger.warn(
                    LogCategory.WORKSPACE,
                    "Refusing to import a workspace onto the reserved store file '${fileNameFor(workspace)}'",
                    mapOf("workspaceId" to workspace.id),
                )
                null
            } else {
                // Save the imported workspace to disk
                scope.launch {
                    withContext(Dispatchers.IO) {
                        fileManager.saveWorkspace(workspace, fileName)
                    }
                    loadedFileNames[workspace.id] = fileName

                    // Update workspaces list (on Main thread), by ID. By NAME an import whose name
                    // matched anything already listed wrote the file and then declined to add the row,
                    // so the user pressed Open from File and saw nothing happen at all.
                    val workspaces = _workspaces.value.toMutableList()
                    val existingIndex = workspaces.indexOfFirst { it.id == workspace.id }
                    if (existingIndex >= 0) workspaces[existingIndex] = workspace else workspaces.add(workspace)
                    _workspaces.value = workspaces
                }

                workspace
            }
        } catch (e: SerializationException) {
            // Import JSON has the same private layout data as an on-disk Space. Do not route the
            // caller-provided document back into logs through the decoder exception.
            logger.warn(LogCategory.WORKSPACE, "Failed to import workspace from JSON", decodeFailure(e))
            null
        } catch (e: Exception) {
            logger.warn(LogCategory.WORKSPACE, "Failed to import workspace from JSON", error = e)
            null
        }

    /**
     * Registers [workspace] in the in-memory picker list without touching disk.
     *
     * For callers that persist the workspace file through their own
     * [WorkspaceFileManager] - the MCP `create_workspace` tool writes through the provider's
     * file manager, the same door `close_workspace` deletes through, and in tests that is a
     * directory the manager's own file manager does not see - but still want the Space visible
     * in the picker for this session. [importWorkspace] would double-write the file.
     */
    fun registerWorkspace(workspace: LayoutWorkspace): LayoutWorkspace {
        // Same door as import: an id registered blank would key this list under "" and later
        // save as ".json". Mint before the workspace enters the list - and hand the registered
        // Space back, since a minted id exists only inside this list until the caller persists
        // it through its own file manager.
        val registered = workspace.withStableId()
        val workspaces = _workspaces.value.toMutableList()
        val existingIndex = workspaces.indexOfFirst { it.id == registered.id }
        if (existingIndex >= 0) workspaces[existingIndex] = registered else workspaces.add(registered)
        _workspaces.value = workspaces
        return registered
    }

    /**
     * Set callback for when a workspace is deleted
     */
    fun setOnWorkspaceDeleted(callback: (String) -> Unit) {
        onWorkspaceDeleted = callback
    }

    /**
     * Delete the Space named [name].
     *
     * **Resolves the name to ONE Space and then works by id.** Two rows can share a name now that
     * the merge keys on ids, and by name this filtered BOTH out of the list while deleting one
     * file and firing `onWorkspaceDeleted` once - so the second Space vanished from every list
     * while its tabs were never torn down and its file stayed on disk. The name form is kept
     * because it is the plugin api's shape (`WorkspaceDataProvider.deleteWorkspace(name)`); a
     * caller that knows which one it means should use [deleteWorkspaceById].
     */
    fun deleteWorkspace(name: String) {
        val workspace = _workspaces.value.find { it.name == name } ?: return
        deleteWorkspaceById(workspace.id)
    }

    /** Delete the Space with [workspaceId]. Refuses a shipped layout, which has no file to delete. */
    fun deleteWorkspaceById(workspaceId: String) {
        scope.launch {
            val workspace = _workspaces.value.find { it.id == workspaceId } ?: return@launch
            // By ID, not by name: a Space merely CALLED "Codex" is the user's and is deletable,
            // where the shipped Codex is not. By name the veto refused both.
            if (!isUserOwnedSpace(workspaceId)) return@launch

            // The same gate the writes take: a Space bound to a reserved store file is refused,
            // not used to delete the record (#1820).
            val fileName = writeTargetFor(workspace)
            if (fileName == null) {
                logger.warn(
                    LogCategory.WORKSPACE,
                    "Refusing to delete the reserved store file '${fileNameFor(workspace)}'",
                    mapOf("workspaceId" to workspaceId),
                )
                return@launch
            }
            val deleted =
                withContext(Dispatchers.IO) {
                    fileManager.deleteWorkspace(fileName)
                }
            if (deleted) {
                // Update state on Main thread
                _workspaces.value = _workspaces.value.filter { it.id != workspaceId }
                loadedFileNames.remove(workspaceId)

                // Notify that workspace was deleted (this will cleanup tabs)
                onWorkspaceDeleted?.invoke(workspaceId)

                // If current workspace was deleted, reset
                if (_currentWorkspace.value?.id == workspaceId) {
                    resetToDefault()
                }
            }
        }
    }

    /**
     * Rename a workspace
     */
    fun renameWorkspace(
        oldName: String,
        newName: String,
    ) {
        val workspace = _workspaces.value.find { it.name == oldName } ?: return
        renameWorkspaceById(workspace.id, newName)
    }

    /**
     * Rename the Space with [workspaceId].
     *
     * **A rename no longer moves a file.** The path comes from the id, so writing the renamed Space
     * to the same file IS the rename - where the name-derived path needed a write-then-delete pair
     * that left the old file behind whenever the write failed. It also means a legacy file keeps
     * its path under a new display name, which is what stops an upgrade renaming anything on disk.
     */
    fun renameWorkspaceById(
        workspaceId: String,
        newName: String,
    ) {
        val existing = _workspaces.value.find { it.id == workspaceId }
        val taken = _workspaces.value.any { it.id != workspaceId && it.name == newName }
        if (taken) {
            logger.debug(LogCategory.WORKSPACE, "Workspace with name already exists", mapOf("name" to newName))
        }
        // Nothing to do for an unknown Space, a name another Space holds, an empty name, or the
        // name it already has.
        val nameIsNew = newName.isNotEmpty() && newName != existing?.name
        if (existing == null || taken || !nameIsNew) return

        scope.launch {
            // By ID: a Space merely CALLED "Codex" is renameable where the shipped Codex is not.
            if (!isUserOwnedSpace(workspaceId)) return@launch

            val renamedWorkspace =
                existing.copy(
                    name = newName,
                    timestamp = Clock.System.now().toEpochMilliseconds(),
                )

            // A rename writes through the same gate: a Space bound to a reserved store file is
            // refused rather than saved over the record under its new name (#1820).
            val fileName = writeTargetFor(renamedWorkspace)
            if (fileName == null) {
                logger.warn(
                    LogCategory.WORKSPACE,
                    "Refusing to rename a Space onto the reserved store file '${fileNameFor(renamedWorkspace)}'",
                    mapOf("workspaceId" to workspaceId),
                )
                return@launch
            }
            val success =
                withContext(Dispatchers.IO) {
                    fileManager.saveWorkspace(renamedWorkspace, fileName) != null
                }

            if (success) {
                loadedFileNames[workspaceId] = fileName
                // Update state on Main thread
                _workspaces.value =
                    _workspaces.value.map {
                        if (it.id == workspaceId) renamedWorkspace else it
                    }

                // If current workspace was renamed, update it
                if (_currentWorkspace.value?.id == workspaceId) {
                    _currentWorkspace.value = renamedWorkspace
                }
            }
        }
    }

    /**
     * Update current workspace with new layout
     */
    fun updateCurrentWorkspace(newWorkspace: LayoutWorkspace) {
        _currentWorkspace.value = newWorkspace
    }

    /**
     * Get the workspace directory path
     */
    fun getWorkspaceDirectory(): String = fileManager.getDefaultWorkspaceDirectory()
}

// Global instance
val workspaceManager = WorkspaceManager()
