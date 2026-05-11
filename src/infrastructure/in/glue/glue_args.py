import sys

def get_glue_args(required_args):
    # noinspection PyUnresolvedReferences
    from awsglue.utils import getResolvedOptions
    return getResolvedOptions(sys.argv, required_args)