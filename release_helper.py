import subprocess


def update_poetry_version(context):
    subprocess.check_call(
        [
            "poetry",
            "version",
            context.get("dev_version", context["new_version"]),
        ]
    )


def ignore_history_file(context):
    context["update_history"] = False


def scriv_collect(context):
    subprocess.check_call(
        ["scriv", "collect", "--version", context["new_version"]]
    )
