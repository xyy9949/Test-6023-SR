#!/usr/bin/python3
from cmath import phase
from ctypes import sizeof
import os
import sched
import sys
import time
from os import path
from subprocess import call
from typing import List
from TT import getState, clearLogFile

def main():
    os.chdir("..")

    # if len(scheduler.split(".")) <= 0:
    #     print("Please enter a valid scheduler name: e.g. explorer.scheduler.NodeFailureInjector")
    #     return
    logFile = "/home/xie/explorer-server/test/test1.txt"
    failPhase = 0
    failRound = 0
    totalPhase = 5
    totalRound = 6
    numTests = 8 # each round has 8 node failure possibilities

    # print(resultFile)
    start_all = time.time()
    resultFile = "/home/xie/explorer-server/test/result.txt"
    preFailNodeId = []
    preFailStateNodeDict = dict()
    preFailNodeStateDict =dict()
    tmpStateDict = dict()


    for j in range(totalPhase):
        for k in range(totalRound):
            # todo: skip messageResponse round
            if k % 2 == 1:
                continue
            preFailNodeId = []
            preFailStateNodeDict = dict()
            if j == 0 and k == 0:
                preFailNodeId = []
            else:
                if k == 0:
                    prePhase = j - 1
                    preRound = 4
                else:
                    prePhase = j
                    # preRound = k - 1
                    preRound = k - 2
                inputPath = "/home/xie/explorer-server/test/failStatePhase" + str(prePhase) + "Round" + str(preRound)
                with open(inputPath, 'r', encoding='utf-8') as infile:
                    for line in infile:
                        data_line_fail_and_state = line.strip("\n").split("|")
                        preFailStateNodeDict[data_line_fail_and_state[1]] = data_line_fail_and_state[0]
                        preFailNodeStateDict[data_line_fail_and_state[0]] = data_line_fail_and_state[1]
                for kk,vv in preFailStateNodeDict.items():
                    preFailNodeId.append(vv)

                # infile.seek(0)
                # infiledata_line_fail_and_state = line.strip("\n").split("|")

            if len(preFailNodeId) != 0:
                for l in range(len(preFailNodeId)):
                    for i in range(1, int(numTests)+1):
                        print("Running test %s" % i)
                        #startB = time.time()
                        if i == 1:
                            failNodeId = "3,3" # no fail in this round
                        elif i == 2:
                            failNodeId = "0,3"
                        elif i == 3:
                            failNodeId = "1,3"
                        elif i == 4:
                            failNodeId = "2,3"
                        elif i == 5:
                            failNodeId = "0-1,3"
                        elif i == 6:
                            failNodeId = "0-2,3"
                        elif i == 7:
                            failNodeId = "1-2,3"
                        else:
                            failNodeId = "0-1-2,3"
                        # if i == 1:
                        #     failNodeId = "3" # no fail in this round
                        # elif i == 2:
                        #     failNodeId = "0"
                        # elif i == 3:
                        #     failNodeId = "1"
                        # elif i == 4:
                        #     failNodeId = "2"
                        # elif i == 5:
                        #     failNodeId = "0-1"
                        # elif i == 6:
                        #     failNodeId = "0-2"
                        # elif i == 7:
                        #     failNodeId = "1-2"
                        # else:
                        #     failNodeId = "0-1-2"
                        # # seed = i + 12345688

                        failNodeId = preFailNodeId[l] + "," + failNodeId
                        scheduler = "explorer.scheduler.NodeFailureInjector"

                        # call("mvn {0} {1} {2}".format("exec:java", "-Dexec.mainClass=explorer.SystemRunner", "-Dexec.args=\"scheduler={0} randomSeed={1} linkEstablishmentPeriod={2} resultFile={3} bugDepth={4}\" ".format(scheduler, str(seed), str(period), resultFile, depth)), shell=True)
                        call("mvn {0} {1} {2}".format("exec:java", "-Dexec.mainClass=explorer.SystemRunner", "-Dexec.args=\"scheduler={0} resultFile={1} failPhase={2} failRound={3} failNodeId={4}\" ".format(scheduler, resultFile, str(j), str(k), failNodeId)), shell=True )

                        result = getState(j, k, logFile)

                        # if k is not 5:
                        #     result.sort(key=lambda x:x[2])

                        result.sort(key=lambda x:x[2])

                        # if len(result) != 0 and k != 5:
                        #     if k == 0 or k == 2 or k == 4:
                        #         tmpList = []
                        #         for ii in range(len(result)):
                        #             tmpList.append([result[ii][0], result[ii][3], result[ii][4]])
                        #         tmpStateDict[failNodeId] = tmpList
                        #     elif k == 1 or k == 3:
                        #         for ii in range(len(result)):
                        #             result[ii][0] = tmpStateDict[preFailNodeId[l]][ii][0]
                        #             result[ii][3] = tmpStateDict[preFailNodeId[l]][ii][1]
                        #             result[ii][4] = tmpStateDict[preFailNodeId[l]][ii][2]
                        # if len(result) != 0:
                        #     tmpList = []
                        #     for ii in range(len(result)):
                        #         tmpList.append([result[ii][0], result[ii][3], result[ii][4]])
                        #     tmpStateDict[failNodeId] = tmpList

                        if len(result) == 0:
                            dis = 4 - k
                            for uu in range(dis):
                                failNodeId = failNodeId + ",3"
                            if len(preFailNodeId)!= 0:
                                tmpState = eval(preFailNodeStateDict[preFailNodeId[l]])
                                for ii in range(len(tmpState)):
                                    result.append([tmpState[ii][0], '4', 'null', tmpState[ii][3], tmpState[ii][4]])
                            fileName = "/home/xie/explorer-server/test/failStatePhase" + str(j) + "Round4"
                        else:
                            if k != 4:
                                tmpState = eval(preFailNodeStateDict[preFailNodeId[l]])
                                for ii in range(len(tmpState)):
                                    if tmpState[ii][0] == "log":
                                        result.append(tmpState[ii])
                            fileName = "/home/xie/explorer-server/test/failStatePhase" + str(j) + "Round" + str(k)


                        # if this phase failed:
                        # if len(result) == 0 or k == 5:
                        #     dis = 5 - k
                        #     for uu in range(dis):
                        #         failNodeId = failNodeId + ",3"
                        #     if k == 1 or k== 3 or k == 5:
                        #         result.append(tmpStateDict[preFailNodeId[l]])
                        #     elif k == 2 or k == 4:
                        #         newPos = preFailNodeId[l].rfind(",")
                        #         result.append(tmpStateDict[preFailNodeId[l][:newPos]])
                        #     fileName = "/home/xie/explorer-server/test/failStatePhase" + str(j) + "Round5"
                        # else:
                        #     fileName = "/home/xie/explorer-server/test/failStatePhase" + str(j) + "Round" + str(k)


                        clearLogFile(logFile)
                        if len(result) is not 0:
                            if not os.path.exists(fileName):
                                os.system(r"touch {}".format(fileName))
                            f = open(fileName, 'a')
                            f.write(failNodeId + "|" + str(result) + "\n")
                            f.close()

            else:
                for i in range(1, int(numTests)+1):
                    print("Running test %s" % i)
                    #startB = time.time()

                    if i == 1:
                        failNodeId = "3,3" # no fail in this round
                    elif i == 2:
                        failNodeId = "0,3"
                    elif i == 3:
                        failNodeId = "1,3"
                    elif i == 4:
                        failNodeId = "2,3"
                    elif i == 5:
                        failNodeId = "0-1,3"
                    elif i == 6:
                        failNodeId = "0-2,3"
                    elif i == 7:
                        failNodeId = "1-2,3"
                    else:
                        failNodeId = "0-1-2,3"
                    # seed = i + 12345688

                    scheduler = "explorer.scheduler.NodeFailureInjector"

                    # call("mvn {0} {1} {2}".format("exec:java", "-Dexec.mainClass=explorer.SystemRunner", "-Dexec.args=\"scheduler={0} randomSeed={1} linkEstablishmentPeriod={2} resultFile={3} bugDepth={4}\" ".format(scheduler, str(seed), str(period), resultFile, depth)), shell=True)
                    call("mvn {0} {1} {2}".format("exec:java", "-Dexec.mainClass=explorer.SystemRunner", "-Dexec.args=\"scheduler={0} resultFile={1} failPhase={2} failRound={3} failNodeId={4}\" ".format(scheduler, resultFile, str(j), str(k), failNodeId)), shell=True )

                    result = getState(j, k, logFile)


                    result.sort(key=lambda x:x[2])

                    # if len(result) != 0:
                    #     tmpList = []
                    #     for ii in range(len(result)):
                    #         tmpList.append([result[ii][0], result[ii][3], result[ii][4]])
                    #     tmpStateDict[failNodeId] = tmpList
                    if len(result) == 0:
                        dis = 4 - k
                        for uu in range(dis):
                            failNodeId = failNodeId + ",3"
                        if len(preFailNodeId)!= 0:
                            tmpState = eval(preFailNodeStateDict[preFailNodeId[l]])
                            for ii in range(len(tmpState)):
                                result.append([tmpState[ii][0], '4', 'null', tmpState[ii][3], tmpState[ii][4]])
                        fileName = "/home/xie/explorer-server/test/failStatePhase" + str(j) + "Round4"
                    else:
                        if k != 4 and k!= 0:
                            tmpState = eval(preFailNodeStateDict[preFailNodeId[l]])
                            for ii in range(len(tmpState)):
                                if tmpState[ii][0] == "log":
                                    result.append(tmpState[ii])
                        fileName = "/home/xie/explorer-server/test/failStatePhase" + str(j) + "Round" + str(k)


                    clearLogFile(logFile)
                    if len(result) != 0:
                        if not os.path.exists(fileName):
                            os.system(r"touch {}".format(fileName))
                        f = open(fileName, 'a')
                        f.write(failNodeId + "|" + str(result) + "\n")
                        f.close()


if __name__ == '__main__':

    main()