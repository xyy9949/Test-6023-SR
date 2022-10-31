#!/usr/bin/python3
import os
import sys
import time
from os import path
from subprocess import call
import string

def getState(requireBallot, requireRound, resultFile):
    f = open(resultFile, 'r')
    data = f.read()
    testList = data.split("\n")
    testList = testList[0 : len(testList) - 1]
    # print(testList)
    currentBallot = -1
    result = []
    for line in testList:
        nodeList = line.split("+")
        if nodeList[0] == "START":
            currentBallot += 1
            continue
        if currentBallot < requireBallot or int(nodeList[1]) < requireRound:
            continue
        if currentBallot > requireBallot or int(nodeList[1]) > requireRound:
            break
        result.append(nodeList)
    f.close()
    return result

def clearLogFile(resultFile):
    f = open(resultFile, 'w')
    f.seek(0)
    f.truncate()



