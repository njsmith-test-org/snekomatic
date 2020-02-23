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

job_info = glom(payload, "client_payload")
print("Job info")
pprint.pprint(job_info)

if mode == "unprivileged":
    print("making artifact")
    Path("worker-artifacts-dir").mkdir()
    Path("worker-artifacts-dir/test").write_text("hello")
else:
    print("reading artifact")
    print("artifact says:", Path("worker-artifacts-dir/test").read_text())
