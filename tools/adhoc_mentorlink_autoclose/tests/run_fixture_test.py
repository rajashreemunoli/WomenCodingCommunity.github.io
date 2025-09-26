import os, sys, tempfile, shutil, subprocess, difflib
from ruamel.yaml import YAML

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SCRIPT = os.path.join(REPO_ROOT, "adhoc_mentorlink_autoclose", "auto_close_mentors.py")
FIXDIR = os.path.join(REPO_ROOT, "adhoc_mentorlink_autoclose", "tests", "fixtures")

def load_yaml(path):
    yaml = YAML()
    with open(path, "r", encoding="utf-8") as f:
        return yaml.load(f)

def main():
    tmpdir = tempfile.mkdtemp(prefix="adhoc_mentorlink_autoclose_")
    try:
        mentors_in = os.path.join(FIXDIR, "mentors.input.yml")
        mentors_expected = os.path.join(FIXDIR, "mentors.expected.yml")
        mentors_tmp = os.path.join(tmpdir, "mentors.yml")
        shutil.copyfile(mentors_in, mentors_tmp)

        env = os.environ.copy()
        env["TIMEZONE"] = "Europe/London"
        env["LOCAL_CSV"] = os.path.join(FIXDIR, "responses.csv")
        env["MENTORS_YML_PATH"] = mentors_tmp
        env["DRY_RUN"] = "0"

        subprocess.run([sys.executable, SCRIPT], check=True, env=env)

        exp_obj = load_yaml(mentors_expected)
        got_obj = load_yaml(mentors_tmp)

        if exp_obj == got_obj:
            print("✅ Test passed (semantic YAML match)")
            sys.exit(0)
        else:
            print("❌ Test failed — YAML objects differ. Showing textual diff for context:\n")
            with open(mentors_expected, "r", encoding="utf-8") as f: exp_txt = f.read().strip()
            with open(mentors_tmp, "r", encoding="utf-8") as f: got_txt = f.read().strip()
            for line in difflib.unified_diff(
                exp_txt.splitlines(), got_txt.splitlines(),
                fromfile="expected", tofile="actual", lineterm=""
            ):
                print(line)
            sys.exit(1)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

if __name__ == "__main__":
    main()