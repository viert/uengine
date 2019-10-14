#!/usr/bin/env python3
import argparse
import os
from jinja2 import Environment, FileSystemLoader

def create_project(args):
    project_name = args.name[0]

    if args.inplace:
        root_dir = "."
    else:
        root_dir = project_name
        if os.path.isdir(project_name):
            raise RuntimeError(f"{project_name} exists")
        os.mkdir(root_dir)

    template_path = os.path.join(os.path.dirname(__file__), "project_source_templates")
    env = Environment(loader=FileSystemLoader(template_path))

    for folder, _, files in os.walk(template_path):
        if "__pycache__" in folder:
            continue
        folder = folder[len(template_path)+1:]
        dst_dir = folder.replace("__appdir__", project_name)
        dst_dir = os.path.join(root_dir, dst_dir)
        if not os.path.isdir(dst_dir):
            os.mkdir(dst_dir)
        for f in files:
            if f.endswith(".pyc"):
                continue
            dst_file = os.path.join(dst_dir, f)
            tmpl = env.get_template(os.path.join(folder, f))
            with open(dst_file, "w") as outf:
                outf.write(tmpl.render(project_name=project_name, auth=True))
    micro_binary = os.path.join(root_dir, "micro.py")
    os.chmod(micro_binary, 0o755)

def main():
    parser = argparse.ArgumentParser(prog="uengine")
    subparsers = parser.add_subparsers(help="sub-command help", dest="action")

    create_parser = subparsers.add_parser("create", help="create a new uengine-based project")
    create_parser.add_argument("name", nargs=1, help="name of your project")
    create_parser.add_argument("-i", "--inplace", dest="inplace", action="store_true", default=False, help="create project structure in current directory instead of [project_name]/")

    args = parser.parse_args()
    if args.action is None:
        parser.print_usage()
        return

    if args.action == "create":
        create_project(args)


if __name__ == '__main__':
    main()