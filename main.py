# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 17:09:28 2023

@author: tarek
"""
import argparse
from pathlib import Path
from matplotlib import pyplot as plt
import numpy as np
import pynwb


def getSessionHandle(filename, directory="data\TestPlitt"):
    # Access specific NWB file
    filepath = Path(directory, filename)

    # Return handle for object containing NWB file
    read_io = pynwb.NWBHDF5IO(filepath, "r")

    return read_io


def printSessionInfo(read_nwbfile):
    # Print out file information. This would be from one session
    print("Subject info:")
    for key, value in read_nwbfile.subject.fields.items():
        print(f"    {key:13}: {value}")

    print("\nSession info:")    
    print(f"    date/time  : {read_nwbfile.session_start_time}")
    print(f"    description: {read_nwbfile.session_description}")

def dataExtraction(read_nwbfile):
    # Neuronal traces from ROIs
    ROI_traces = read_nwbfile.processing["ophys"]["Fluorescence"]["RoiResponseSeries"].data
    nFrames, nNeurons = ROI_traces.shape

    # Deconvolved activity traces
    deconvTraces = read_nwbfile.processing["ophys"]["Fluorescence"]["Deconvolved"].data
    
    # information of start time for every trial
    tstartData = read_nwbfile.processing["behavior"]["BehavioralTimeSeries"]["tstart"].data[()]
    nTrials = int(sum(tstartData))

    # Matrix that stores where the starting points for each trial is
    # Useful to divide trials
    startIndices = np.where(tstartData)[0]
    
    # morph value for each trial (total morph taken as base morph + jitter)
    baseMorph = read_nwbfile.stimulus["morph"].data[()]
    wallJitter = read_nwbfile.stimulus["wallJitter"].data[()]
    totalMorph = baseMorph + wallJitter

    # Position of the rat over the session
    position = read_nwbfile.processing["behavior"]["BehavioralTimeSeries"]["pos"].data[()]
    
    return nFrames, nNeurons, deconvTraces, tstartData, nTrials, startIndices, baseMorph, totalMorph, position

def trialize(data, pos, startIndices):
    # Take the data from a session (nFrames x nNeurons) and transform into (nTrials x trial_length x nNeurons)
    
    # Adding the last index so the last trial is included
    myIndices = np.append(startIndices, len(data))
    
    datablocks = [data[myIndices[i]:myIndices[i+1]] for i in range(len(myIndices)-1)]
    posblocks = [pos[myIndices[i]:myIndices[i+1]] for i in range(len(myIndices)-1)]
    np.array(datablocks,dtype=object)
    np.array(posblocks,dtype=object)
    
    return datablocks, posblocks

def positionalBin(datablocks, posblocks): 
    # This positional binning function takes in the output from trialize
    nTrials = len(datablocks)
    nNeurons = np.shape(datablocks[0])[1] # since dblocks is nTrials x trial_length x nNeurons, take second size of 0th trial
    bins = np.arange(0, 4.51, 0.1)
    accResp = np.zeros((nTrials,len(bins),nNeurons)) # New dimension added, nNeurons
    counterMat = np.zeros((nTrials,len(bins)-1))
    
    for i in range(nTrials):
        # Array of indices indicating which bin each timepoint belongs to, and count of how many elements per bin
        binInds = np.digitize(posblocks[i],bins) - 1
        counterMat[i,:] = np.histogram(posblocks[i],bins)[0]
        
        # accumulate all values in bin
        np.add.at(accResp[i,:,:], binInds, datablocks[i][:])
        
    
    return accResp[:,:-1,:], counterMat

def getSpatialInformation(df, occp, baseMorphList):
    # This function calculates the spatial information for one cell for each of the 5 different baseMorph values
    
    # Find what base morph each trial corresponds
    baseIndices = {0: np.where(baseMorphList == 0)[0],
                   0.25: np.where(baseMorphList == 0.25)[0],
                   0.5: np.where(baseMorphList == 0.5)[0],
                   0.75: np.where(baseMorphList == 0.75)[0],
                   1: np.where(baseMorphList == 1)[0]}
    
    # Finding the average trial for the given cell for each base morph value 
    # and calculating the partial occupancy raatio for each sptial bin
    nBins = 45
    aggBase = np.zeros([5, nBins])
    aggOccBase = np.zeros([5, nBins])
    
    for i, key in enumerate(baseIndices):
        baseInds = baseIndices[key]
        aggBase[i,:] = np.sum(df[baseInds,:], axis=0) / np.sum(occp[baseInds,:], axis=0)
        aggOccBase[i,:] = np.sum(occp[baseInds,:], axis=0) / np.sum(occp[baseInds,:])
    
    
    # Loop to calculate the spatial information metric for the given cell
    SI = np.zeros(5)
    for i, key in enumerate(baseIndices):
        lmbda = np.mean(aggBase[i,:])
        logTerm = np.log2((aggBase[i,:]+1e-5)/lmbda)
        SIPre = (aggOccBase[i,:] * aggBase[i,:] * logTerm)
        SI[i] = np.sum(SIPre)
        
    return SI   
    
    

if __name__ == "__main__":

    argparser = argparse.ArgumentParser()

    argparser.add_argument("--filename", default="sub-R2_ses-20190219T210000_behavior+ophys_small.nwb")
    argparser.add_argument("--directory", default="data\TestPlitt")

    args = argparser.parse_args()

    with getSessionHandle(filename=args.filename, directory=args.directory) as read_io:
        R2 = read_io.read()

        nFrames, nNeurons, deconvTraces, tstartData, nTrials, startIndices, baseMorph, totalMorph, position = dataExtraction(R2)
        dbs, pbs = trialize(deconvTraces, position, startIndices)
        df, occp = positionalBin(dbs, pbs)
        
        SIMatrix = np.zeros((nNeurons, 5))
        baseMorphList = baseMorph[startIndices]
        
        for i in range(nNeurons):
            SIMatrix[i,:] = getSpatialInformation(df[:,:,i], occp, baseMorphList)
            # print(SIMatrix[i,:])
        
        print(SIMatrix[0,:])
        # import pdb; pdb.set_trace()



