import os
import sys
import subprocess
import yaml
import time
import shutil
from scripts._helpers import set_PROJdir
set_PROJdir()

with open('config.yaml') as f:
    config = yaml.safe_load(f)

# Scripts are run in the order laid out below. Running this script will execute any of the scripts below, so to run
# only part of the pipeline, change the settings from True to False.
# IMPORTANT NOTE: Consider each step in the list below as dependent on those previous, e.g. you must run build_shapes
# before you will be able to run base_network. Scripts in the same step are not dependent on each other.

# Step 0: Create the directory where the project files will be stored
build_project_folders = True

# Step 1: Build country and regional polygons
build_shapes = True

# Step 2: Build the base network
base_network = True

# Step 3: Build Voronoi cells to represent each node's coverage area
build_bus_regions = True

# Step 4: Calculate node-level data for the base network
build_renewable_profiles = True
build_hydro_profile = True
build_powerplants = True

# Step 5: Attach the information from steps 3 & 4 to each node in the base network
add_electricity = True

# Step 6: Simplify the base network (cleaning required prior to clustering)
simplify_network = True

# Step 7 (optional): Prepare TSO matching
prepare_tso_busmap = True

# Step 8: Cluster the network
cluster_network = True


############################################# DO NOT EDIT BELOW THIS LINE #############################################
script_dict = {'build_project_folders.py':build_project_folders,
               'build_shapes.py':build_shapes,
               'base_network.py':base_network,
               'build_bus_regions.py':build_bus_regions,
               'build_renewable_profiles.py':build_renewable_profiles,
               'build_hydro_profile.py':build_hydro_profile,
               'build_powerplants.py':build_powerplants,
               'add_electricity.py':add_electricity,
               'simplify_network.py':simplify_network,
               'prepare_tso_busmap.py':prepare_tso_busmap,
               'cluster_network.py':cluster_network}

if __name__ == "__main__":
    os.chdir("scripts")
    timestarted_all = time.time()

    if os.path.exists("../models/" + config['project_folder'] + "/config_archived.yaml"):
        with open("../models/" + config['project_folder'] + "/config_archived.yaml") as f:
            config_archived = yaml.safe_load(f)
        if config != config_archived:
            overwrite_bool = input("Attempting to duplicate and archive config.yaml at models/" + config['project_folder'] +
                                   "/config_archived.yaml but file already exists and does not match current config.yaml. "
                                   "Confirm overwrite? Y/N: ")
            if not overwrite_bool.capitalize() in ["Y","N"]:
                print('Please run script again and respond with "Y" or "N"')
                sys.exit()
            else:
                if overwrite_bool.capitalize() == "N":
                    print("config_archived.yaml not overwritten.")
                else:
                    print("config_archived.yaml updated with latest parameters.")
                    shutil.copyfile("../config.yaml", "../models/" + config['project_folder'] + "/config_archived.yaml")

    for scriptname in script_dict:
        if script_dict.get(scriptname, False):
            print("Executing " + scriptname)
            timestarted_sub = time.time()
            if scriptname == 'prepare_tso_busmap.py':
                os.chdir("../models/" + config['project_folder'] + "/tso_clustering")
                subprocess.call(['python', scriptname])
                os.chdir("../../../scripts")
            else: subprocess.call(['python', scriptname])
            timetaken_sub = round(time.time() - timestarted_sub, 1)
            if timetaken_sub < 60:
                print("Completed " + scriptname + " in " + str(timetaken_sub) + " seconds.")
            elif timetaken_sub < 3600:
                timetaken_sub = round(timetaken_sub / 60, 1)
                print("Completed " + scriptname + " in " + str(timetaken_sub) + " minutes.")
            else:
                timetaken_sub = round(timetaken_sub / 3600, 2)
                print("Completed " + scriptname + " in " + str(timetaken_sub) + " hours.")
        else:
            print("Skipping " + scriptname)

    timetaken_all = round(time.time() - timestarted_all, 1)
    if timetaken_all < 60:
        print("Completed running all scripts in " + str(timetaken_all) + " seconds.")
    elif timetaken_all < 3600:
        timetaken_all = round(timetaken_all / 60, 1)
        print("Completed running all scripts in " + str(timetaken_all) + " minutes.")
    else:
        timetaken_all = round(timetaken_all / 3600, 2)
        print("Completed running all scripts in " + str(timetaken_all) + " hours.")

