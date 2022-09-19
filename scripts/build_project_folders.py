import yaml
import os
import sys
import shutil

with open('../config.yaml') as f:
    config = yaml.safe_load(f)

project_folder = config['project_folder']

def create_directory(projectname):
    os.makedirs("../models/" + projectname + "/intermediate_files")
    os.makedirs("../models/" + projectname + "/networks")

    if config['enable'].get('tso_busmap', False):
        print("Custom TSO clustering desired - creating template folder. Please copy the desired TSO service area shapefile into the tso_clustering subfolder and update prepare_tso_busmap.py accordingly.")
        os.makedirs("../models/" + projectname + "/tso_clustering")
    return

if __name__ == "__main__":
    if os.path.exists("../models/" + project_folder):
        overwrite_bool = input("A project folder already exists at models/" + project_folder + ". Confirm overwrite? All files in subfolder will be deleted. Y/N: ")
        if not overwrite_bool.capitalize() in ["Y","N"]:
            print('Please run script again and respond with "Y" or "N"')
            sys.exit()
        else:
            if overwrite_bool.capitalize() == "N":
                print("Exiting...")
                sys.exit()
            else:
                print("Overwriting project subfolder at models/" + project_folder + "...")
                shutil.rmtree("../models/" + project_folder)
                create_directory(project_folder)

    else:
        print("Project folder does not exist - creating subfolders at models/" + project_folder)
        create_directory(project_folder)
