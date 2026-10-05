package ai.rever.boss.components.workspaces

import ai.rever.boss.plugin.workspace.PanelConfig
import ai.rever.boss.plugin.workspace.SplitConfig.SinglePanel
import ai.rever.boss.plugin.workspace.TabConfig
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.test.setMain
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * The reserved store records must not be reachable through a Space's write target (#1820).
 *
 * `WorkspaceManager.fileNameFor` resolves a Space to the file it was loaded from, or to
 * `<id>.json`. A file on disk NAMED like a record can still carry a foreign id - only the
 * document records are scan-skipped, so a corrupt or hand-edited `Last_Session.json` /
 * `last-session.json` loads as an ordinary Space - and before `writeTargetFor` existed that
 * binding routed every writer onto the record: `saveCurrentWorkspace` rewrote it,
 * `renameWorkspaceById` rewrote it, `deleteWorkspaceById` deleted it.
 *
 * The one id allowed through the gate is the record's own - `last-session` legitimately writes
 * `last-session.json`, and a record loaded off a legacy install keeps writing
 * `Last_Session.json` - so the tests below pin the allowance as hard as the refusals.
 *
 * Driven through the real manager on the `SaveRebindSeamTest` pattern: a test Main, a temp
 * directory, and a bounded real-time wait for the IO write.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class ReservedStoreWriteTargetTest {
    private fun managerIn(directory: String) = WorkspaceManager(fileManager = WorkspaceFileManager(directory))

    /**
     * Drive the test Main until [predicate] holds, waiting real time for the manager's IO write
     * to land in between. Bounded so a wedged write fails the test instead of hanging it.
     */
    private fun TestScope.awaitMain(predicate: () -> Boolean) {
        val deadline = System.currentTimeMillis() + 15_000
        while (!predicate()) {
            assertTrue(
                System.currentTimeMillis() < deadline,
                "the manager's Main-scope work did not land within 15 seconds",
            )
            Thread.sleep(20)
            runCurrent()
        }
    }

    /**
     * Wait for the manager's constructor load to land: `init` ends with an unconditional
     * `_workspaces.value = merge...`, which would drop a row a test committed first.
     */
    private fun TestScope.awaitLoadSettled(manager: WorkspaceManager) {
        awaitMain { manager.workspaces.value.size >= PredefinedWorkspaces.allWorkspaces.size }
    }

    /** A refusal lands inside the launched coroutine; pump long enough for a real write too. */
    private fun TestScope.pump() {
        repeat(10) {
            Thread.sleep(20)
            runCurrent()
        }
    }

    private fun space(
        id: String,
        name: String,
    ) = LayoutWorkspace(
        id = id,
        name = name,
        description = "$name description",
        layout =
            SinglePanel(
                PanelConfig(
                    id = "$id-panel",
                    tabs = listOf(TabConfig(type = "terminal", title = "t")),
                ),
            ),
        timestamp = 1L,
    )

    /**
     * The collision at the heart of #1820: `Last_Session.json` carrying an ordinary id loads as
     * a Space - but must not be bound to it. Pre-fix the scan recorded
     * `loadedFileNames["victim-space"] = "Last_Session.json"` and the save below rewrote the
     * record; post-fix the Space saves under its own `<id>.json` and the record keeps its bytes.
     */
    @Test
    fun `a record-named file with a foreign id loads as a Space but is never bound to it`(
        @TempDir directory: File,
    ) = runTest {
        Dispatchers.setMain(StandardTestDispatcher(testScheduler))
        try {
            val recordFile = File(directory, LEGACY_LAST_SESSION_FILE)
            recordFile.writeText(WorkspaceSerializer.serialize(space("victim-space", "Victim")))
            val seededBytes = recordFile.readText()

            val manager = managerIn(directory.absolutePath)
            awaitMain { manager.workspaces.value.any { it.id == "victim-space" } }
            assertEquals(seededBytes, recordFile.readText(), "loading must not touch the record's file")

            manager.updateCurrentWorkspace(manager.workspaces.value.single { it.id == "victim-space" })
            var saved: LayoutWorkspace? = null
            manager.saveCurrentWorkspace(name = null, onSaved = { saved = it })
            awaitMain { saved != null }

            assertEquals(
                seededBytes,
                recordFile.readText(),
                "the save must land on the Space's own file, not the session record",
            )
            assertTrue(
                File(directory, WorkspaceFileManagerCommon.fileNameForId("victim-space")).exists(),
                "the Space saves under `<id>.json` once the reserved binding is refused",
            )
        } finally {
            Dispatchers.resetMain()
        }
    }

    /**
     * The same collision without an id to pin it on: a blank-id `last-session.json` used to get
     * a minted id written back INTO the record file at scan time, then the binding routed saves
     * there too. Both writes are refused now; the Space still loads and still saves - under its
     * minted id's own file.
     */
    @Test
    fun `a record file carrying no id still mints one, and the record file is never written`(
        @TempDir directory: File,
    ) = runTest {
        Dispatchers.setMain(StandardTestDispatcher(testScheduler))
        try {
            val recordFile = File(directory, WorkspaceFileManagerCommon.fileNameForId(LAST_SESSION_ID))
            recordFile.writeText(WorkspaceSerializer.serialize(space("", "Corrupt Record")))
            val seededBytes = recordFile.readText()

            val manager = managerIn(directory.absolutePath)
            awaitMain { manager.workspaces.value.any { it.name == "Corrupt Record" } }
            assertEquals(
                seededBytes,
                recordFile.readText(),
                "the minted id must not be persisted into the reserved file",
            )

            val phantom = manager.workspaces.value.single { it.name == "Corrupt Record" }
            assertTrue(phantom.id.isNotBlank(), "a blank id is still minted to a stable one")
            assertFalse(phantom.id.equals(LAST_SESSION_ID, ignoreCase = true), "the minted id is not the record's")

            manager.updateCurrentWorkspace(phantom)
            var saved: LayoutWorkspace? = null
            manager.saveCurrentWorkspace(name = null, onSaved = { saved = it })
            awaitMain { saved != null }

            assertEquals(seededBytes, recordFile.readText(), "saving the loaded Space must not touch the record")
            assertTrue(File(directory, WorkspaceFileManagerCommon.fileNameForId(phantom.id)).exists())
        } finally {
            Dispatchers.resetMain()
        }
    }

    /**
     * The gate the other way in: a Space that was never loaded from disk (`registerWorkspace`
     * records no file for it) whose ID itself resolves onto the theme store. `fileNameForId`
     * maps `Space_Themes` to `Space_Themes.json`, so save must refuse before the write - and
     * report through `onFailed`, the channel the button turns into a status message.
     */
    @Test
    fun `saving a Space whose id resolves to the theme store is refused and reported`(
        @TempDir directory: File,
    ) = runTest {
        Dispatchers.setMain(StandardTestDispatcher(testScheduler))
        try {
            val store = File(directory, SPACE_THEMES_FILE).apply { writeText("""{"themes":{"a":1}}""") }
            val seededBytes = store.readText()
            val manager = managerIn(directory.absolutePath)
            awaitLoadSettled(manager)

            manager.updateCurrentWorkspace(manager.registerWorkspace(space("Space_Themes", "Rogue")))
            var savedCount = 0
            var failedName: String? = null
            manager.saveCurrentWorkspace(
                name = null,
                onSaved = { savedCount++ },
                onFailed = { failedName = it },
            )
            awaitMain { failedName != null || savedCount > 0 }

            assertEquals("Rogue", failedName, "the refusal is reported to the caller")
            assertEquals(0, savedCount, "a refused save fires no success callback")
            assertEquals(seededBytes, store.readText(), "the theme store is never written")
        } finally {
            Dispatchers.resetMain()
        }
    }

    /** A rename used to write the record under the Space's new name. Refused whole now. */
    @Test
    fun `renaming a Space whose id resolves to the theme store is refused`(
        @TempDir directory: File,
    ) = runTest {
        Dispatchers.setMain(StandardTestDispatcher(testScheduler))
        try {
            val store = File(directory, SPACE_THEMES_FILE).apply { writeText("""{"themes":{"a":1}}""") }
            val seededBytes = store.readText()
            val manager = managerIn(directory.absolutePath)
            awaitLoadSettled(manager)
            manager.registerWorkspace(space("Space_Themes", "Rogue"))

            manager.renameWorkspaceById("Space_Themes", "Renamed Rogue")
            pump()

            assertEquals(seededBytes, store.readText(), "the rename must not write the record's file")
            assertEquals(
                "Rogue",
                manager.workspaces.value
                    .single { it.id == "Space_Themes" }
                    .name,
                "a refused rename changes nothing",
            )
        } finally {
            Dispatchers.resetMain()
        }
    }

    /** And delete used to remove the record outright. Refused whole now. */
    @Test
    fun `deleting a Space whose id resolves to the theme store is refused`(
        @TempDir directory: File,
    ) = runTest {
        Dispatchers.setMain(StandardTestDispatcher(testScheduler))
        try {
            val store = File(directory, SPACE_THEMES_FILE).apply { writeText("""{"themes":{"a":1}}""") }
            val seededBytes = store.readText()
            val manager = managerIn(directory.absolutePath)
            awaitLoadSettled(manager)
            manager.registerWorkspace(space("Space_Themes", "Rogue"))

            manager.deleteWorkspaceById("Space_Themes")
            pump()

            assertTrue(store.exists(), "the delete must not remove the record")
            assertEquals(seededBytes, store.readText())
            assertTrue(
                manager.workspaces.value.any { it.id == "Space_Themes" },
                "a refused delete keeps the Space in the list - its file is still there",
            )
        } finally {
            Dispatchers.resetMain()
        }
    }

    /**
     * The allowance the gate is shaped around: the session record's own id writes
     * `last-session.json`, because that IS the record. A gate that refused every reserved name
     * would break crash recovery; this pins that it does not.
     */
    @Test
    fun `the session record's own write still lands`(
        @TempDir directory: File,
    ) = runTest {
        Dispatchers.setMain(StandardTestDispatcher(testScheduler))
        try {
            val manager = managerIn(directory.absolutePath)
            awaitLoadSettled(manager)

            val record = asLastSession(space("ignored", "Live")).copy(timestamp = 7L)
            assertTrue(manager.writeLastSessionRecordBlocking(record))

            val recordFile = File(directory, WorkspaceFileManagerCommon.fileNameForId(LAST_SESSION_ID))
            assertTrue(recordFile.exists(), "the record writes its own file")
            assertEquals(LAST_SESSION_ID, WorkspaceSerializer.deserialize(recordFile.readText()).id)
        } finally {
            Dispatchers.resetMain()
        }
    }

    /**
     * The other half of the allowance: a record LOADED from a legacy `Last_Session.json` keeps
     * that file. The scan binds `last-session` to it legitimately, and the write goes there -
     * the binding that must survive is exactly the one the refusal is careful not to break.
     */
    @Test
    fun `a record loaded from the legacy file keeps writing it`(
        @TempDir directory: File,
    ) = runTest {
        Dispatchers.setMain(StandardTestDispatcher(testScheduler))
        try {
            val legacyFile = File(directory, LEGACY_LAST_SESSION_FILE)
            legacyFile.writeText(
                WorkspaceSerializer.serialize(asLastSession(space("x", "Old")).copy(timestamp = 3L)),
            )
            val manager = managerIn(directory.absolutePath)
            awaitMain { manager.workspaces.value.any { it.id == LAST_SESSION_ID } }

            val record = asLastSession(space("y", "New")).copy(timestamp = 9L)
            assertTrue(manager.writeLastSessionRecordBlocking(record))

            assertEquals(
                9L,
                WorkspaceSerializer.deserialize(legacyFile.readText()).timestamp,
                "the record writes through the legacy file it was loaded from",
            )
            assertFalse(
                File(directory, WorkspaceFileManagerCommon.fileNameForId(LAST_SESSION_ID)).exists(),
                "and does not sprout a second record file",
            )
        } finally {
            Dispatchers.resetMain()
        }
    }

    /**
     * The pin for the gate's coverage: every writer that can put bytes on - or remove - a
     * workspace-store file resolves its target through `writeTargetFor`, and the helper consults
     * the reserved list. Source-checked because a writer that skipped the gate would fail no
     * test above unless a fixture happened to reach it.
     */
    @Test
    fun `every workspace writer resolves its target through the reserved-store gate`() {
        val relative = "composeApp/src/commonMain/kotlin/ai/rever/boss/components/workspaces/WorkspaceManager.kt"
        val source =
            checkNotNull(
                generateSequence(File(".").absoluteFile) { it.parentFile }
                    .map { File(it, relative) }
                    .firstOrNull { it.isFile }
                    ?.readText(),
            ) { "could not find $relative" }
        val gateBody = source.substringAfter("private fun isReservedTargetForOtherId(").substringBefore("\n    }")
        assertTrue(
            "reservedWorkspaceStoreFileName(" in gateBody,
            "the shared write-target predicate must consult the reserved store list",
        )
        listOf(
            "internal fun saveCurrentWorkspace(",
            "fun writeLastSessionRecordBlocking(",
            "fun saveLastSessionBlocking(",
            "fun importWorkspace(",
            "fun deleteWorkspaceById(",
            "fun renameWorkspaceById(",
        ).forEach { declaration ->
            val body = source.substringAfter(declaration).substringBefore("\n    fun ")
            assertTrue("writeTargetFor(" in body, "$declaration must resolve its write target through writeTargetFor")
        }
        // The scan resolves its own write targets (the id write-back, the ".json" adoption)
        // rather than calling writeTargetFor, but it must consult the same predicate - and
        // must not record the binding that routes a Space's writes onto a record.
        val scanBody =
            source.substringAfter("private suspend fun adoptScannedWorkspace(").substringBefore("\n    /**")
        assertTrue(
            "isReservedTargetForOtherId(" in scanBody,
            "the scan's adoption path must consult the same reserved-target predicate",
        )
    }
}
