# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 17:09:28 2023

@author: tarek
"""

from pathlib import Path
from matplotlib import pyplot as plt
import numpy as np
import pynwb

def importSession(filename):
    # Import specific NWB file
    filepath = Path("data\TestPlitt", filename)

    # Create handle for object containing NWB file
    read_io = pynwb.NWBHDF5IO(filepath, "r")
    read_nwbfile = read_io.read()
    return read_nwbfile


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


R2 = importSession("sub-R2_ses-20190219T210000_behavior+ophys_small.nwb")
tstartData = R2.processing["behavior"]["BehavioralTimeSeries"]["tstart"].data

# nFrames, nNeurons, deconvTraces, tstartData, nTrials, startIndices, baseMorph, totalMorph, position = dataExtraction(R2)
# dbs, pbs = trialize(deconvTraces, position, startIndices)
# df, occp = positionalBin(dbs, pbs)

print(type(tstartData))



