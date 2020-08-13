import os
from pathlib import Path


def makedir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
    return


def leafStruc(base):
    makedir(base)
    if base[-1] != "/":
        base = base + "/"
    nrt = base + "nrt/"
    makedir(nrt)
    rep = base + "rep/"
    makedir(rep)
    metadata = base + "metadata/"
    makedir(metadata)
    stats = base + "stats/"
    makedir(stats)
    doc = base + "doc/"
    makedir(doc)
    code = base + "code/"
    makedir(code)
    return nrt, rep, metadata, stats, doc, code


"""synced dropbox path"""
# vault = str(Path.home()) + r"/CMAP Data Submission Dropbox/Simons CMAP/vault/"
# staging = str(Path.home()) + r"/CMAP Data Submission Dropbox/Simons CMAP/staging/"
# collected_data = (
#     str(Path.home()) + r"/CMAP Data Submission Dropbox/Simons CMAP/collected_data/"
# )
# download_transfer = (
#     str(Path.home()) + r"/CMAP Data Submission Dropbox/Simons CMAP/download_transfer/"
# )
"""mounted NAS - dropbox sync paths"""

vault = str(Path.home()) + r"/Vault/vault/"
staging = str(Path.home()) + r"/Vault/staging/"
collected_data = str(Path.home()) + r"/Vault/collected_data/"
download_transfer = str(Path.home()) + r"/Vault/download_transfer/"


################# Download Transfer Structure  ##################
makedir(download_transfer)

################# Collected Data Structure  ##################
makedir(collected_data)


################# Staging Area Structure  ##################
makedir(staging)

#########  data  #########
data = staging + "data/"
makedir(data)

########  metadata  ########
metadata = staging + "metadata/"
makedir(metadata)

########  combined (temporary)  ########
combined = staging + "combined/"
makedir(combined)


################## Simons CMAP vault structure ##################
makedir(vault)

#########  BCP  #########
BCP = vault + "BCP/"
makedir(BCP)

#########  assimilation  #########
assimilation = vault + "assimilation/"
makedir(assimilation)


###########  models  ##########
model = vault + "model/"
makedir(model)

#########  r2r2 cruise #########
r2r_cruise = vault + "r2r_cruise/"
makedir(r2r_cruise)

#########  observations  #########
obs = vault + "observation/"
makedir(obs)
in_situ = obs + "in-situ/"
makedir(in_situ)
remote = obs + "remote/"
makedir(remote)

#########  obs/in-situ/cruise  #########
cruise = in_situ + "cruise/"
makedir(cruise)

#########  obs/in-situ/station  #########
station = in_situ + "station/"
makedir(station)

#########  obs/in-situ/float  #########
float = in_situ + "float/"
makedir(float)

########  obs/remote/satellite  ########
satellite = remote + "satellite/"
makedir(satellite)
