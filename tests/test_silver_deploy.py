"""Exercise the real installer activation tail with sandboxed paths/systemd."""
import shlex
import subprocess
import tempfile
import unittest
from pathlib import Path


class SilverDeployTests(unittest.TestCase):
    def exercise(self, existing=False, active=False, fail=False, mode='--resume'):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        base, release, units = root/'base', root/'base/new', root/'units'
        units.mkdir()
        (release/'app/systemd').mkdir(parents=True)
        for name in ('loxone-silver-refresh.service', 'loxone-silver-refresh.timer'):
            (release/'app/systemd'/name).write_text('new unit\n')
            if existing:
                (units/name).write_text('old admin unit\n')
        if existing:
            (base/'old').mkdir()
            (base/'current').symlink_to(base/'old')
        secret = root/'silver.env'
        secret.write_text('unchanged-token-placeholder\n')
        if active:
            (root/'active').touch()
        script = Path('scripts/install-silver.sh').read_text()
        tail = script[script.index('# First installation'):]
        tail = tail.replace('/etc/systemd/system', str(units))
        # The deployed tail only imports local resources; mock that command here.
        preamble = f'''set -Eeuo pipefail
base={shlex.quote(str(base))}
release={shlex.quote(str(release))}
mock_root={shlex.quote(str(root))}
mode={shlex.quote(mode)}
expected_sha=0123456789012345678901234567890123456789
install() {{
  local -a args=()
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -o|-g) shift 2 ;;
      *) args+=("$1"); shift ;;
    esac
  done
  command install "${{args[@]}}"
}}
systemctl() {{
  echo "$*" >> "$mock_root/calls"
  case "$1" in
    is-active) [[ -e "$mock_root/active" ]] ;;
    cat) [[ -f "$mock_root/units/loxone-silver-refresh.timer" ]] ;;
    stop) [[ ! -e "$mock_root/active" ]] || unlink "$mock_root/active" ;;
    start) touch "$mock_root/active" ;;
    show) echo inactive ;;
    daemon-reload) return 0 ;;
    *) return 99 ;;
  esac
}}
runuser() {{ return {7 if fail else 0}; }}
'''
        result = subprocess.run(['bash', '-c', preamble+tail], text=True,
                                capture_output=True, timeout=5)
        self.assertEqual(secret.read_text(), 'unchanged-token-placeholder\n')
        return result, root

    def test_first_install_stays_stopped(self):
        result, root = self.exercise()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((root/'active').exists())
        self.assertEqual((root/'base/current').resolve(), root/'base/new')

    def test_active_timer_resumes_on_upgrade(self):
        result, root = self.exercise(existing=True, active=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((root/'active').exists())
        self.assertEqual((root/'base/previous').resolve(), root/'base/old')

    def test_stopped_timer_is_not_enabled(self):
        result, root = self.exercise(existing=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((root/'active').exists())

    def test_manual_pause_mode_preserved(self):
        result, root = self.exercise(existing=True, active=True, mode='--pause')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((root/'active').exists())

    def test_failed_upgrade_restores_code_units_and_timer(self):
        result, root = self.exercise(existing=True, active=True, fail=True)
        self.assertEqual(result.returncode, 7, result.stderr)
        self.assertEqual((root/'base/current').resolve(), root/'base/old')
        self.assertTrue((root/'active').exists())
        self.assertEqual((root/'units/loxone-silver-refresh.service').read_text(), 'old admin unit\n')

    def test_failed_first_activation_removes_only_new_links_and_units(self):
        result, root = self.exercise(fail=True)
        self.assertEqual(result.returncode, 7, result.stderr)
        self.assertFalse((root/'base/current').is_symlink())
        self.assertFalse((root/'units/loxone-silver-refresh.service').exists())
        self.assertTrue((root/'base/new/app').exists())

    def test_bridge_finishes_bronze_before_silver(self):
        script = Path('scripts/deploy-from-github.sh').read_text()
        call = script.index('bash "$app_dir/scripts/install-silver.sh"')
        self.assertLess(script.index('trap - ERR\necho "Deployment completed'), call)
        self.assertLess(script.index('printf \'%s\\n\' "$expected_sha"'), call)
        self.assertIn('"$expected_sha" --resume "$repo"', script[call:])
        workflow = Path('.github/workflows/deploy.yml').read_text()
        self.assertIn('timeout-minutes: 30', workflow)
        self.assertIn('loxone-bronze-deploy-v3', workflow)
        installer = Path('scripts/install-silver.sh').read_text()
        self.assertIn('active|activating|deactivating|reloading', installer)
        self.assertNotIn('systemctl enable', installer)
        self.assertIn('if [[ ! -e /etc/loxone-silver/silver.env ]]', installer)
