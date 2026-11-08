"""The lifecycle of the scheduled mailbox check of an agent: activation, settings update, deactivation, startup and
destroy of the agent, and the job as the White Rabbit stores and runs it.

Run from the core root, with the secrets the core requires (any value for the tests):
``CAT_CRYPTO_KEY=test CAT_CRYPTO_SALT=test python -m pytest cat/plugins/ccat-email-monitoring/tests``.

The Cat imports every ``.py`` file of the plugin, tests included: at import time this module needs only the stdlib,
the Cat and the plugin are loaded in ``setUpClass``.
"""
import asyncio
import sys
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


class FakeRedis:
    """What ``RedisJobStore`` uses of Redis: the jobs as pickled bytes in a hash, their run times in a sorted set."""

    def __init__(self):
        self.hashes = {}
        self.zsets = {}
        self.connection_pool = MagicMock()  # disconnected on shutdown

    def hexists(self, key, field):
        return field in self.hashes.get(key, {})

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def hset(self, key, field, value):
        assert isinstance(value, bytes), "the job store keeps bytes"
        self.hashes.setdefault(key, {})[field] = value

    def hdel(self, key, *fields):
        for field in fields:
            self.hashes.get(key, {}).pop(field, None)

    def zadd(self, key, mapping):
        self.zsets.setdefault(key, {}).update(mapping)

    def zrem(self, key, *members):
        for member in members:
            self.zsets.get(key, {}).pop(member, None)

    def multi(self):
        pass

    def execute(self):
        pass

    def pipeline(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class WhiteRabbit:
    """The White Rabbit of the core on its job store, APScheduler's ``RedisJobStore`` (the job pickled; Redis faked),
    shared by the nodes; ``schedule_interval_job`` adds the job as the core does. Its locks (``acquire_lock``) are
    the ones of the checks."""

    def __init__(self):
        from datetime import timezone

        from apscheduler.jobstores.redis import RedisJobStore
        from apscheduler.schedulers.background import BackgroundScheduler

        self.store = RedisJobStore(jobs_key="white_rabbit:jobs", run_times_key="white_rabbit:run_times")
        self.store.redis = FakeRedis()
        self.scheduler = BackgroundScheduler(jobstores={"default": self.store}, timezone=timezone.utc)
        self.scheduler.start(paused=True)
        self.scheduled = []
        self.locks = set()

    def get_job(self, job_id):
        return self.store.lookup_job(job_id)

    def schedule_interval_job(self, job, job_id=None, minutes=0, **kwargs):
        self.scheduler.add_job(job, "interval", id=job_id, minutes=minutes, kwargs=kwargs, replace_existing=True)
        self.scheduled.append(job_id)
        return job_id

    def remove_job(self, job_id):
        self.scheduler.remove_job(job_id)
        return True

    def acquire_lock(self, event):
        if event in self.locks:
            return False
        self.locks.add(event)
        return True

    def release_lock(self, event):
        self.locks.discard(event)

    def shutdown(self):
        self.scheduler.shutdown(wait=False)


class Agent:
    """A Cheshire Cat as the plugin sees it: ``mad_hatter.plugins`` holds only its active plugins (as the core's), and
    ``load_settings`` needs the agent named (the core would guess it from the stack)."""

    def __init__(self, agent_id="agent", active=True, **saved):
        self.agent_key = agent_id
        self.saved = {"imap_username": "u", "imap_password": "segreta-123"} | saved
        self.mad_hatter = MagicMock()
        self.activate(active)

    def activate(self, active):
        plugin = MagicMock()
        plugin.load_settings = self.load_settings
        self.mad_hatter.active_plugins = [MODULE.PLUGIN_ID] if active else []
        self.mad_hatter.plugins = {MODULE.PLUGIN_ID: plugin} if active else {}

    async def load_settings(self, agent_id):
        assert agent_id == self.agent_key, "the settings of this agent"
        return dict(self.saved)


class TestScheduledJob(unittest.TestCase):
    """The job of the periodic check lives in the job store of the White Rabbit (Redis, pickled), shared by the nodes:
    it holds only the id of the agent; every check gets the agent and its settings of that moment."""

    @classmethod
    def setUpClass(cls):
        _load()

    def setUp(self):
        self.white_rabbit = WhiteRabbit()
        self.addCleanup(self.white_rabbit.shutdown)
        self.agents = {}
        self.lizard = MagicMock(white_rabbit=self.white_rabbit)
        self.lizard.get_cheshire_cat = AsyncMock(side_effect=self.get_cheshire_cat)

    async def get_cheshire_cat(self, agent_id):
        """As the core: an agent that does not exist raises; one without its settings is None."""
        from cat.exceptions import UnknownAgentException

        if agent_id not in self.agents:
            raise UnknownAgentException("Bad Request")
        return self.agents[agent_id]

    def job_id(self, agent_id="agent"):
        return f"ccat_email_monitoring:{agent_id}"

    def job(self, agent_id="agent"):
        return self.white_rabbit.store.lookup_job(self.job_id(agent_id))

    def hook(self, name, *args):
        with patch.object(MODULE, "BillTheLizard", return_value=self.lizard), \
                patch.object(MODULE, "JOB_POLL_SECONDS", 0.01):
            return asyncio.run(_with_heartbeat(HOOKS[name](*args)))

    def activate(self, cat):
        self.agents[cat.agent_key] = cat
        cat.activate(True)
        self.hook("after_plugin_toggling_on_agent", MODULE.PLUGIN_ID, cat)

    def run_job(self, agent_id="agent"):
        """What the White Rabbit of a node does at every interval: the job read back from its store, then called."""
        job = self.job(agent_id)
        with patch.object(MODULE, "BillTheLizard", return_value=self.lizard), \
                patch.object(MODULE, "_run_check", AsyncMock()) as run_check:
            asyncio.run(job.func(*job.args, **job.kwargs))
        return run_check

    def test_the_activation_stores_a_job_that_checks_with_the_settings_of_now(self):
        cat = Agent(poll_interval_minutes=7)
        self.activate(cat)
        self.assertEqual(self.job().trigger.interval.total_seconds(), 7 * 60)
        self.assertEqual(self.job().kwargs, {"agent_id": "agent"})
        stored = self.white_rabbit.store.redis.hget("white_rabbit:jobs", self.job_id())
        self.assertNotIn(b"segreta-123", stored, "the password stays encrypted at rest")

        cat.saved["imap_password"] = "nuova-456"  # saved again meanwhile: the next check uses it
        run_check = self.run_job()
        run_check.assert_awaited_once()
        _, settings, checked = run_check.await_args.args
        self.assertIs(checked, cat)
        self.assertEqual((settings.imap_password, settings.poll_interval_minutes), ("nuova-456", 7))
        self.assertEqual(self.white_rabbit.locks, set(), "the lock is released")

    def test_a_job_already_scheduled_is_kept(self):
        cat = Agent()
        self.activate(cat)
        self.activate(cat)
        self.assertEqual(self.white_rabbit.scheduled, [self.job_id()])

    def test_saved_settings_not_valid_schedule_nothing(self):
        """E.g. saved by a version of the plugin with wider limits: the error is logged, the check is not scheduled."""
        with patch.object(MODULE.log, "error") as error:
            self.activate(Agent(poll_interval_minutes=0))
        self.assertIsNone(self.job())
        error.assert_called_once()

    def test_a_check_with_nothing_to_do_does_not_check(self):
        cat = Agent()
        self.activate(cat)
        for case in ("agent without its settings", "plugin not active", "another check running"):
            with self.subTest(case=case):
                self.agents["agent"] = None if case == "agent without its settings" else cat
                cat.activate(case != "plugin not active")  # e.g. deactivated for the whole instance: the job stays
                if case == "another check running":
                    self.white_rabbit.acquire_lock(self.job_id())
                self.lizard.get_cheshire_cat.reset_mock()
                self.run_job().assert_not_awaited()
                if case == "another check running":
                    self.lizard.get_cheshire_cat.assert_not_awaited()  # the agent is not loaded for nothing
                    self.white_rabbit.release_lock(self.job_id())
                self.assertEqual(self.white_rabbit.locks, set())
        self.assertIsNotNone(self.job())

    def test_the_deactivation_removes_the_job_without_waiting_for_a_running_check(self):
        """The running check ends by itself; the lock keeps a new one from starting meanwhile."""
        cat = Agent()
        self.activate(cat)
        self.white_rabbit.acquire_lock(self.job_id())  # a check running, on any node
        cat.activate(False)
        self.hook("after_plugin_toggling_on_agent", MODULE.PLUGIN_ID, cat)
        self.assertIsNone(self.job())
        self.hook("after_plugin_toggling_on_agent", MODULE.PLUGIN_ID, cat)  # no job: nothing to remove
        self.assertIsNone(self.job())

    def test_the_settings_update_schedules_the_new_interval_with_the_plugin_active_only(self):
        cat = Agent()
        self.activate(cat)
        self.white_rabbit.acquire_lock(self.job_id())  # a check running: not waited for
        self.hook("after_plugin_settings_update", MODULE.PLUGIN_ID, cat.saved | {"poll_interval_minutes": 9}, cat)
        self.assertEqual(self.job().trigger.interval.total_seconds(), 9 * 60)
        self.assertEqual(self.job().kwargs, {"agent_id": "agent"})

        cat.activate(False)
        self.hook("after_plugin_toggling_on_agent", MODULE.PLUGIN_ID, cat)
        self.hook("after_plugin_settings_update", MODULE.PLUGIN_ID, cat.saved, cat)
        self.assertIsNone(self.job(), "saved while not active: no check")

    def test_the_hooks_of_another_plugin_are_ignored(self):
        cat = Agent(active=False)
        self.white_rabbit.schedule_interval_job(MODULE._check_mailbox, self.job_id(), minutes=5, agent_id="agent")
        self.hook("after_plugin_toggling_on_agent", "another", cat)
        self.hook("after_plugin_settings_update", "another", {"poll_interval_minutes": 9}, Agent())
        self.assertEqual(self.job().trigger.interval.total_seconds(), 5 * 60)

    def test_the_startup_aligns_the_jobs_with_the_agents(self):
        """The White Rabbit removed the jobs of the node when it shut down: the agents with the plugin active get theirs
        again, a job left to an agent without it goes; an agent failing does not stop the others."""
        self.white_rabbit.schedule_interval_job(MODULE._check_mailbox, self.job_id("spento"), minutes=5,
                                                agent_id="spento")
        agents = {"agent": Agent("agent", poll_interval_minutes=3), "spento": Agent("spento", active=False),
                  "rotto": Agent("rotto")}

        async def load_settings(agent_id):
            if agent_id == "rotto":
                raise ConnectionError("redis unreachable")
            return await agents[agent_id].load_settings(agent_id)

        self.lizard.plugin_manager.plugins = {MODULE.PLUGIN_ID: MagicMock(load_settings=load_settings)}
        crud_agents = MagicMock(get_agents_main_keys=AsyncMock(return_value=["rotto", "agent", "spento"]))
        crud_plugins = MagicMock(get_active_plugins_from_db=AsyncMock(
            side_effect=lambda agent_id: agents[agent_id].mad_hatter.active_plugins))
        with patch.object(MODULE, "crud_agents", crud_agents), patch.object(MODULE, "crud_plugins", crud_plugins), \
                patch.object(MODULE.log, "error") as error:
            self.hook("after_lizard_bootstrap", self.lizard)
        self.assertEqual(self.job("agent").trigger.interval.total_seconds(), 3 * 60)
        self.assertIsNone(self.job("spento"))
        self.assertIsNone(self.job("rotto"))
        error.assert_called_once()

        with patch.object(MODULE, "crud_agents", crud_agents):
            self.hook("after_lizard_bootstrap", MagicMock(white_rabbit=None))  # no scheduler: nothing to align
        crud_agents.get_agents_main_keys.assert_awaited_once()

    def test_the_destroy_of_the_agent_removes_its_job_and_then_its_state(self):
        """The state of the folders is out of the keys of the agent the core destroys: an agent created again with the
        same id would skip the emails it remembers. A running check (its lock, on any node) saves it: the state goes
        once it ends, the event loop serving the other requests meanwhile."""
        cat = Agent()
        self.activate(cat)
        self.white_rabbit.acquire_lock(self.job_id())  # a check running
        events = []
        crud = MagicMock(delete=AsyncMock(side_effect=lambda key: events.append(("state deleted", key, set(
            self.white_rabbit.locks)))))

        async def check_ends():
            await asyncio.sleep(0.2)
            events.append(("check ended",))
            self.white_rabbit.release_lock(self.job_id())

        async def destroy():
            ending = asyncio.create_task(check_ends())
            await HOOKS["after_cheshire_cat_destroy"]("agent", cat)
            await ending

        with patch.object(MODULE, "BillTheLizard", return_value=self.lizard), \
                patch.object(MODULE, "crud_settings", crud), patch.object(MODULE, "JOB_POLL_SECONDS", 0.01):
            ticks = asyncio.run(_with_heartbeat(destroy()))
        self.assertIsNone(self.job())
        self.assertEqual(events, [("check ended",),
                                  ("state deleted", "plugins:ccat_email_monitoring:agent:seen", {self.job_id()})])
        self.assertEqual(self.white_rabbit.locks, set(), "the lock taken to delete the state is released")
        self.assertGreater(ticks, 10)

    def test_the_destroy_waits_a_bounded_time_for_a_check_that_does_not_end(self):
        """E.g. the lock of a check whose node died, which stays until it expires (an hour)."""
        self.white_rabbit.acquire_lock(self.job_id())
        crud = MagicMock(delete=AsyncMock())
        with patch.object(MODULE, "BillTheLizard", return_value=self.lizard), \
                patch.object(MODULE, "crud_settings", crud), patch.object(MODULE, "JOB_POLL_SECONDS", 0.01), \
                patch.object(MODULE, "DESTROY_WAIT_SECONDS", 0.05), patch.object(MODULE.log, "warning") as warning:
            asyncio.run(HOOKS["after_cheshire_cat_destroy"]("agent", Agent()))
        crud.delete.assert_awaited_once_with("plugins:ccat_email_monitoring:agent:seen")
        warning.assert_called_once()
        self.assertEqual(self.white_rabbit.locks, {self.job_id()}, "the lock of the other check is not released")


class TestScheduleStateMachine(unittest.TestCase):
    """The lifecycle of the jobs of the agents, explored: agents created and destroyed, the plugin activated and
    deactivated on them, settings saved, checks running (their lock) and run by the White Rabbit, the instance
    restarted (its node shut down: the White Rabbit removes the jobs it scheduled). Rules checked at every step."""

    @classmethod
    def setUpClass(cls):
        _load()

    def test_invariants(self):
        from hypothesis import HealthCheck, settings, strategies as st
        from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

        case = self
        ids = st.sampled_from(["a", "b"])

        class Machine(RuleBasedStateMachine):
            def __init__(self):
                super().__init__()
                self.white_rabbit = WhiteRabbit()
                self.agents = {}
                self.state = {}  # the keys of the plugin's own state
                self.destroyed_with_the_plugin = set()

                async def get_cheshire_cat(agent_id):
                    return self.agents[agent_id]

                async def load_settings(agent_id):  # the plugin of the system: the settings of the agent named
                    return dict(self.agents[agent_id].saved)

                self.lizard = MagicMock(white_rabbit=self.white_rabbit, get_cheshire_cat=get_cheshire_cat)
                self.lizard.plugin_manager.plugins = {MODULE.PLUGIN_ID: MagicMock(load_settings=load_settings)}

            def teardown(self):
                self.white_rabbit.shutdown()

            def call(self, coro_fn, *args):
                machine = self

                async def delete(key):
                    machine.state.pop(key, None)

                async def run_check(lizard, settings, cat):  # a check saves the state of the folders
                    machine.state[f"plugins:ccat_email_monitoring:{cat.agent_key}:seen"] = settings.imap_username

                async def agents():
                    return list(machine.agents)

                async def active(agent_id):
                    return list(machine.agents[agent_id].mad_hatter.active_plugins)

                with patch.object(MODULE, "BillTheLizard", return_value=self.lizard), \
                        patch.object(MODULE, "crud_settings", MagicMock(delete=delete)), \
                        patch.object(MODULE, "crud_agents", MagicMock(get_agents_main_keys=agents)), \
                        patch.object(MODULE, "crud_plugins", MagicMock(get_active_plugins_from_db=active)), \
                        patch.object(MODULE, "_run_check", run_check), \
                        patch.object(MODULE, "JOB_POLL_SECONDS", 0.001), \
                        patch.object(MODULE, "DESTROY_WAIT_SECONDS", 0.003):
                    asyncio.run(coro_fn(*args))

            def job(self, agent_id):
                return self.white_rabbit.store.lookup_job(f"ccat_email_monitoring:{agent_id}")

            def active(self, agent_id):
                return agent_id in self.agents and MODULE.PLUGIN_ID in self.agents[agent_id].mad_hatter.active_plugins

            # -- operations --
            @rule(agent_id=ids)
            def create(self, agent_id):
                if agent_id not in self.agents:
                    self.agents[agent_id] = Agent(agent_id, active=False)
                    self.destroyed_with_the_plugin.discard(agent_id)

            @rule(agent_id=ids)
            def toggle(self, agent_id):
                if agent_id in self.agents:
                    cat = self.agents[agent_id]
                    cat.activate(not self.active(agent_id))
                    self.call(HOOKS["after_plugin_toggling_on_agent"], MODULE.PLUGIN_ID, cat)

            @rule(agent_id=ids, minutes=st.integers(1, 3))
            def save_settings(self, agent_id, minutes):
                if agent_id in self.agents:
                    cat = self.agents[agent_id]
                    cat.saved["poll_interval_minutes"] = minutes
                    if self.active(agent_id):  # the hooks of the plugins active on the agent
                        self.call(HOOKS["after_plugin_settings_update"], MODULE.PLUGIN_ID, dict(cat.saved), cat)

            @rule(agent_id=ids)
            def check_starts_or_ends(self, agent_id):  # a check running on some node holds the lock
                job_id = f"ccat_email_monitoring:{agent_id}"
                if not self.white_rabbit.acquire_lock(job_id):
                    self.white_rabbit.release_lock(job_id)

            @rule(agent_id=ids)
            def run_job(self, agent_id):
                job = self.job(agent_id)
                if job is not None:
                    self.call(lambda: job.func(*job.args, **job.kwargs))

            @rule()
            def restart(self):
                """The node shuts down (its White Rabbit removes the jobs it scheduled), then starts again; the locks
                of the checks it ran stay until they expire."""
                for job_id in list(self.white_rabbit.store.redis.hashes.get("white_rabbit:jobs", {})):
                    self.white_rabbit.remove_job(job_id)
                self.call(HOOKS["after_lizard_bootstrap"], self.lizard)

            @rule(agent_id=ids)
            def destroy(self, agent_id):
                if agent_id not in self.agents:
                    return
                cat = self.agents.pop(agent_id)
                if MODULE.PLUGIN_ID in cat.mad_hatter.active_plugins:  # the hooks of the plugins active on it
                    self.destroyed_with_the_plugin.add(agent_id)
                    self.call(HOOKS["after_cheshire_cat_destroy"], agent_id, cat)

            # -- rules --
            @invariant()
            def a_job_for_every_agent_with_the_plugin_active_and_none_else(self):
                for agent_id in ("a", "b"):
                    case.assertEqual(self.job(agent_id) is not None, self.active(agent_id), agent_id)

            @invariant()
            def the_job_holds_the_agent_and_the_interval_of_its_settings(self):
                for agent_id in ("a", "b"):
                    job = self.job(agent_id)
                    if job is not None:
                        case.assertEqual(job.kwargs, {"agent_id": agent_id})
                        minutes = self.agents[agent_id].saved.get("poll_interval_minutes", 5)
                        case.assertEqual(job.trigger.interval.total_seconds(), minutes * 60)
                for stored in self.white_rabbit.store.redis.hashes.get("white_rabbit:jobs", {}).values():
                    case.assertNotIn(b"segreta-123", stored)

            @invariant()
            def nothing_left_of_an_agent_destroyed_with_the_plugin(self):
                for agent_id in self.destroyed_with_the_plugin:
                    case.assertNotIn(f"plugins:ccat_email_monitoring:{agent_id}:seen", self.state)

        Machine.TestCase.settings = settings(max_examples=150, stateful_step_count=25, deadline=None,
                                             suppress_health_check=[HealthCheck.too_slow])
        Machine.TestCase("runTest").runTest()


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
