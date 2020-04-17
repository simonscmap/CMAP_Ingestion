#!/usr/bin/env python

import os
from pathlib import Path

def makedir(directory):
	if not os.path.exists(directory):
		os.makedirs(directory)
	return

def leafStruc(base):
	nrt = base + 'nrt/'
	makedir(nrt)
	rep = base + 'rep/'
	makedir(rep)
	metadata = base + 'metadata/'
	makedir(metadata)
	stats = base + 'stats/'
	makedir(stats)
	doc = base + 'doc/'
	makedir(doc)
	code = base + 'code/'
	makedir(code)
	return nrt, rep, metadata, stats, doc, code

vault = str(Path.home()) + r'/CMAP_Dropbox/vault/'


################## Simons CMAP vault structure ##################
makedir(vault)

#########  assimilation  #########
assimilation = vault + 'assimilation/'
makedir(assimilation)


###########  models  ##########
model = vault + 'model/'
makedir(model)

#########  observations  #########
obs = vault + 'observation/'
makedir(obs)
in_situ = obs + 'in-situ/'
makedir(in_situ)
remote = obs + 'remote/'
makedir(remote)

#########  obs/in-situ/cruise  #########
cruise = in_situ + 'cruise/'
makedir(cruise)

#########  obs/in-situ/station  #########
station = in_situ + 'station/'
makedir(station)

#########  obs/in-situ/float  #########
float = in_situ + 'float/'
makedir(float)

########  obs/remote/satellite  ########
satellite = remote + 'satellite/'
makedir(satellite)

####  cruise_tree_test #####
cruise_tree = cruise + 'tblCruiseName_CruiseNickName_Data/'
makedir(cruise_tree)
nrt_cruise_tree, rep_cruise_tree, metadata_cruise_tree, stats_cruise_tree, doc_cruise_tree, code_cruise_tree = leafStruc(cruise_tree)
