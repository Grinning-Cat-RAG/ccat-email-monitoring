"""The replacement of the scheduled mailbox check waits for a running check without blocking the event loop of the
instance (every agent and chat it serves).

Run from the core root: ``python -m pytest cat/plugins/ccat-email-monitoring/tests``.

The Cat imports every ``.py`` file of the plugin, tests included: at import time this module needs only the stdlib,
the Cat and the plugin are loaded in ``setUpClass``.
"""
import asyncio
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

PLUGIN_DIR = Path(__file__).resolve().parents[1]
LOADED = set()

MODULE = None
HOOKS = {}


def _load():
    """Loads the plugin once, from the test classes, as the Cat loads it: whatever the name of its folder."""
    global MODULE
    if str(PLUGIN_DIR) in LOADED:
        return
    from cat.looking_glass.mad_hatter.plugin import Plugin

    plugin = Plugin(str(PLUGIN_DIR))
    plugin._load_decorated_functions()
    HOOKS.update({h.name: h.function for h in plugin.hooks})
    MODULE = sys.modules[HOOKS["after_plugin_toggling_on_agent"].__module__]
    LOADED.add(str(PLUGIN_DIR))


async def _with_heartbeat(coro):
    """Run ``coro`` and count how many times another coroutine ran meanwhile (every 10 ms)."""
    ticks = 0

    async def heartbeat():
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    beat = asyncio.create_task(heartbeat())
    try:
        await coro
    finally:
        beat.cancel()
    return ticks


class WhiteRabbit:
    """The scheduler of the core: the job of the agent runs for ``running`` more checks, then it is scheduled."""

    def __init__(self, running=0, exists=True):
        from cat.core_plugins.white_rabbit.white_rabbit import JobStatus

        self.status = JobStatus
        self.running = running
        self.exists = exists
        self.removed = []
        self.checks = 0

    def get_job(self, job_id):
        self.checks += 1
        if not self.exists:
            return None
        if self.running > 0:
            self.running -= 1
            return MagicMock(status=self.status.RUNNING)
        return MagicMock(status=self.status.SCHEDULED)

    def remove_job(self, job_id):
        self.removed.append(job_id)
        self.exists = False


def _agent(active):
    cat = MagicMock()
    cat.agent_key = "agent"
    cat.mad_hatter.get_plugin.return_value.id = "ccat_email_monitoring"
    cat.mad_hatter.active_plugins = ["ccat_email_monitoring"] if active else []
    return cat


class TestScheduleReplacement(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _load()

    def run_hook(self, name, *args, white_rabbit):
        lizard = MagicMock(white_rabbit=white_rabbit)
        with patch.object(MODULE, "BillTheLizard", return_value=lizard), \
                patch.object(MODULE, "JOB_POLL_SECONDS", 0.05, create=True):
            return asyncio.run(_with_heartbeat(HOOKS[name](*args)))

    def test_waiting_for_a_running_check_never_blocks_the_event_loop(self):
        """Regression: the replacement waited for a running check with ``time.sleep(5)`` inside the asynchronous
        hooks: the event loop, and so every request served by the instance, was blocked while the check ran."""
        white_rabbit = WhiteRabbit(running=3)
        begin = time.monotonic()
        ticks = self.run_hook("after_plugin_toggling_on_agent", "ccat_email_monitoring", _agent(False),
                              white_rabbit=white_rabbit)
        self.assertLess(time.monotonic() - begin, 1.0)
        self.assertGreater(ticks, 8, "other coroutines run while the check is running")
        self.assertEqual(white_rabbit.removed, ["ccat_email_monitoring:agent"])
        self.assertEqual(white_rabbit.checks, 4)

    def test_the_settings_update_replaces_the_job(self):
        white_rabbit = WhiteRabbit(running=2)
        cat = _agent(True)
        with patch.object(MODULE, "_setup_email_monitor_schedule", AsyncMock()) as setup:
            ticks = self.run_hook("after_plugin_settings_update", "ccat_email_monitoring", {}, cat,
                                  white_rabbit=white_rabbit)
        self.assertGreater(ticks, 5)
        self.assertEqual(white_rabbit.removed, ["ccat_email_monitoring:agent"])
        setup.assert_awaited_once_with(cat, "ccat_email_monitoring:agent")

    def test_no_job_nothing_to_remove(self):
        white_rabbit = WhiteRabbit(exists=False)
        self.run_hook("after_plugin_toggling_on_agent", "ccat_email_monitoring", _agent(False),
                      white_rabbit=white_rabbit)
        self.assertEqual(white_rabbit.removed, [])

    def test_the_activation_schedules_the_check(self):
        cat = _agent(True)
        white_rabbit = WhiteRabbit()
        with patch.object(MODULE, "_setup_email_monitor_schedule", AsyncMock()) as setup:
            self.run_hook("after_plugin_toggling_on_agent", "ccat_email_monitoring", cat, white_rabbit=white_rabbit)
        setup.assert_awaited_once_with(cat, "ccat_email_monitoring:agent")
        self.assertEqual(white_rabbit.removed, [])

    def test_the_hooks_of_another_plugin_are_ignored(self):
        white_rabbit = WhiteRabbit()
        for name, args in (("after_plugin_toggling_on_agent", ("another", _agent(False))),
                           ("after_plugin_settings_update", ("another", {}, _agent(True)))):
            with self.subTest(hook=name):
                self.run_hook(name, *args, white_rabbit=white_rabbit)
        self.assertEqual((white_rabbit.checks, white_rabbit.removed), (0, []))


class TestPluginSafety(unittest.TestCase):
    def test_every_file_passes_the_scanner_of_the_core(self):
        from cat.looking_glass.mad_hatter.plugin_extractor import PluginExtractor

        self.assertTrue(PluginExtractor._is_safe_plugin(str(PLUGIN_DIR)))

    def test_tests_import_only_the_stdlib_at_import_time(self):
        import ast

        for test_file in sorted(Path(__file__).parent.glob("*.py")):
            tree = ast.parse(test_file.read_text(encoding="utf-8"))
            for node in tree.body:
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    names = [a.name for a in node.names] if isinstance(node, ast.Import) else [node.module or ""]
                    for name in names:
                        self.assertIn(name.split(".")[0], sys.stdlib_module_names, (test_file.name, name))


if __name__ == "__main__":
    unittest.main()
