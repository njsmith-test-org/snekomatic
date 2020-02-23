import sys
import os
import json
import glom
from pathlib import Path
import pprint

mode = sys.argv[1]
print("Mode is:", mode)
print("Kicked off by:", os.environ["GITHUB_ACTOR"])

payload = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
print("Payload:")
pprint.pprint(payload)

if mode == "unprivileged":
    print("making artifact")
    Path("artifacts/test").write_text("hello")
else:
    print("reading artifact")
    print("artifact says:", Path("artifacts/test").read_text())
