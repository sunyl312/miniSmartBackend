from Sqlite3Utilis import sqlite3db
from UserPackage import UserPackage
import argparse
import sys

sq = sqlite3db("GSA2SRA.db")

sra = UserPackage("gsa2sra")


def fileTransfer(CRAacc,status):
    if status=="GOOD":
        sq.updateTASKtable("FQ_STATUS",2,CRAacc)
        print("good")
    elif status=="BAD":
        sq.updateTASKtable("FQ_STATUS",3,CRAacc)
        print("bad")

fileTransfer(CRAacc=sys.argv[1],status=sys.argv[2])

