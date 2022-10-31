#!/usr/bin/python3
import os
from re import A
import sys
import time
from os import path
from subprocess import call
import string

from TT import getState


if __name__ == '__main__':
    os.chdir("..")
    scheduler = "explorer.scheduler.NodeFailureInjector"
    # TODO:use any empty txt file to replace!
    resultFile = "/home/xie/explorer-server/test/result.txt"

    failNodeId = "2,3,2,3,0-2,3,3,3,0,3,1-2,3"
    
    call("mvn {0} {1} {2}".format("exec:java", "-Dexec.mainClass=explorer.SystemRunner", "-Dexec.args=\"scheduler={0} resultFile={1} failPhase={2} failRound={3} failNodeId={4}\" ".format(scheduler, resultFile, str(0), str(0), failNodeId)), shell=True )

    