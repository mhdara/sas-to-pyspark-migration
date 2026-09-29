"""SASPy connection settings for SAS OnDemand for Academics (ODA).

Set REGION to the home region shown on your ODA dashboard (welcome.oda.sas.com).
The password is NOT here: SASPy reads it from ~/.authinfo, a line of the form
    oda user <your ODA user id or email> password <your password>
(file readable only by you: chmod 600 ~/.authinfo).
"""

import os
import shutil

REGION = "us2"  # us1 | us2 | eu1 | ap1 | ap2

_HOSTS = {  # ODA workspace servers per home region (SASPy documentation, "ODA" configuration)
    "us1": [f"odaws0{i}-usw2.oda.sas.com" for i in range(1, 5)],
    "us2": [f"odaws0{i}-usw2-2.oda.sas.com" for i in range(1, 3)],
    "eu1": [f"odaws0{i}-euw1.oda.sas.com" for i in range(1, 3)],
    "ap1": [f"odaws0{i}-apse1.oda.sas.com" for i in range(1, 3)],
    "ap2": [f"odaws0{i}-apse1-2.oda.sas.com" for i in range(1, 3)],
}

# Java 17 from JAVA_HOME when set, else whatever `java` is on the PATH
_java = os.path.join(os.environ["JAVA_HOME"], "bin", "java") if os.environ.get("JAVA_HOME") else None
_java = _java if _java and os.path.exists(_java) else shutil.which("java") or "java"

SAS_config_names = ["oda"]

oda = {
    "java": _java,
    "iomhost": _HOSTS[REGION],
    "iomport": 8591,
    "authkey": "oda",
    "encoding": "utf-8",  # the ODA session is UTF-8: keeps é, è, ç intact in logs and data
}
