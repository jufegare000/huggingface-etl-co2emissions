import sys
import json
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "partition_id", "input_path", "thread_count"]
)

print(json.dumps({
    "job_name": args["JOB_NAME"],
    "partition_id": args["partition_id"],
    "input_path": args["input_path"],
    "thread_count": args["thread_count"]
}))