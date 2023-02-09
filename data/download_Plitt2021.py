#!/usr/bin/env python
"""
Adapted from https://pynwb.readthedocs.io/en/stable/tutorials/advanced_io/streaming.html

Downloads and saves partial NWB files from a dataset, after popping out their large acquisition module.
"""

import argparse
import glob
from pathlib import Path
import shutil
import time
import warnings

import h5py
from hdmf.backends.hdf5.h5_utils import H5DataIO
from dandi import dandiapi
from dandi import download as dandi_download
import pynwb
from pynwb.ophys import Fluorescence, RoiResponseSeries



def get_dandiset_asset_info(dandiset_id="000054", version="draft", part=""):
    """
    get_dandiset_asset_info()
    
    Retrieves URLs and file names for the dataset.
    """
    
    client = dandiapi.DandiAPIClient()
    dandi = client.get_dandiset(dandiset_id, version)
    
    asset_urls, asset_content_urls, asset_filenames = [], [], []
    for asset in dandi.get_assets():
        asset_urls.append(asset.download_url)
        asset_content_urls.append(asset.get_content_url(follow_redirects=1, strip_query=True))
        asset_filenames.append(Path(asset.path).name)
    
    sort_order = [asset_filenames.index(name) for name in sorted(asset_filenames) if part in name]
    asset_urls = [asset_urls[i] for i in sort_order]
    asset_content_urls = [asset_content_urls[i] for i in sort_order]
    asset_filenames = [asset_filenames[i] for i in sort_order]

    return asset_urls, asset_content_urls, asset_filenames


def pop_acquisition(read_nwbfile):
    """
    pop_acquisition()
    
    Pop out acquisition module, in place. Can save around 28 GB per file.
    """
    
    read_nwbfile.acquisition.pop("TwoPhotonSeries")
    
    
def compress_roi_data(read_nwbfile):
    """
    compress_roi_data()
    
    Replaces ROI data with a compressed version, in place. (May not be worth it as it may save only 300 MB per file.) 
    """
    
    fluo = read_nwbfile.processing["ophys"].data_interfaces.pop("Fluorescence")    
    new_fluo = Fluorescence(name="Fluorescence")
    read_nwbfile.processing["ophys"].add(new_fluo)
    for name in fluo.roi_response_series:
        series = fluo[name]
        data = H5DataIO(data=series.data[()], compression=True)
        new_series = RoiResponseSeries(
                name=name,
                comments=series.comments,
                conversion=series.conversion,
                data=data,
                description=series.description,
                rate=series.rate,
                resolution=series.resolution,
                rois=series.rois,
                starting_time=series.starting_time,
    #            starting_time_unit=series.starting_time_unit,
                unit=series.unit,
        )
        new_fluo.add_roi_response_series(new_series)
        

def fix_(read_nwbfile):
    """
    fix_()
    
    Replaces Image segmentation data with a compressed version, in place. (May not be worth it as it may save only 300 MB per file.) 
    """
    
    fluo = read_nwbfile.processing["ophys"].data_interfaces.pop("Fluorescence")    
    new_fluo = Fluorescence(name="Fluorescence")
    read_nwbfile.processing["ophys"].add(new_fluo)
    for name in fluo.roi_response_series:
        series = fluo[name]
        data = H5DataIO(data=series.data[()], compression=True)
        new_series = RoiResponseSeries(
                name=name,
                comments=series.comments,
                conversion=series.conversion,
                data=data,
                description=series.description,
                rate=series.rate,
                resolution=series.resolution,
                rois=series.rois,
                starting_time=series.starting_time,
    #            starting_time_unit=series.starting_time_unit,
                unit=series.unit,
        )
        new_fluo.add_roi_response_series(new_series)


def create_new_file(load_info, filename, compress=False, output=None):
    """
    create_new_file(load_info, filename)
    
    Creates a new file, storing only the smaller version.
    """
    
    compress_str = "_comp" if compress else ""
    filename_out = str(filename).replace("ophys.", f"ophys_small{compress_str}.")
    if output is not None:
        filename_out = Path(output, Path(filename_out).name)
    print(f"    Creating {filename_out}...")
    with pynwb.NWBHDF5IO(**load_info) as read_io:
        read_nwbfile = read_io.read()
        pop_acquisition(read_nwbfile)
        if compress:
            compress_roi_data(read_nwbfile)
        with pynwb.NWBHDF5IO(str(filename_out), "w") as write_io:
            write_io.export(src_io=read_io, nwbfile=read_nwbfile,  write_args={"link_data": False})


def download_and_replace(asset_url, filename, output=".", compress=False, tempdir=None, n_str=""):
    """
    download_and_replace(asset_url, filename)
    
    Creates a new file by downloading the full size, then replacing it with the smaller version.
    """
    
    if tempdir is None:
        tempdir = output
    Path(tempdir).mkdir(exist_ok=True, parents=True)
    Path(output).mkdir(exist_ok=True, parents=True)
    new_filename = Path(output, filename)
    
    print(f"File{n_str}: {filename}:")
    existing_files = glob.glob(str(Path(output, "*.nwb")))
    stem = Path(filename).stem
    if sum([stem in existing_file for existing_file in existing_files]):
        print("    Skipping. File already exists.")
        return
    
    print("    Downloading...")
    dandi_download.download(asset_url, tempdir, existing="refresh")
    
    load_info = {"path": str(new_filename), "mode": "r"}
    create_new_file(load_info, new_filename, compress=compress, output=output)
    
    print(f"    Deleting {filename}...")
    Path(tempdir, filename).unlink()

   
def stream_and_save(asset_url, filename, output=".", compress=False, fs=None, n_str=""):
    """
    stream_and_save(asset_url, filename)
    
    Creates a new file by opening it for streaming, then storing only the smaller version.
    """
    
    Path(output).mkdir(exist_ok=True, parents=True)
    new_filename = Path(output, filename)
    
    print(f"{n_str}File: {filename}:")
    existing_files = glob.glob(str(Path(output, "*.nwb")))
    stem = Path(filename).stem
    if sum([stem in existing_file for existing_file in existing_files]):
        print("    Skipping. File already exists.")
        return

    if fs is None:
        load_info = {"path": asset_url, "mode": "r", "load_namespaces": True, "driver": "ros3"}
        create_new_file(load_info, new_filename, compress=compress, output=output)

    else:    
        with fs.open(asset_url, "rb") as f:
            with h5py.File(f) as h5_handle:
                load_info = {"file": h5_handle, "load_namespaces": True}           
                create_new_file(load_info, new_filename, compress=compress, output=output) 
        fs.clear_cache() # appears critical to avoid a memory leak
                        

def main(output=".", compress=False, tempdir=None, stream=False, try_ros3=False, part=""):
    """
    main()
    
    Runs through the full dataset.
    """

    overall_start = time.perf_counter()

    if not stream:
        print("Warning: This will be take much longer (e.g., 10x) if downloading the full data files, instead of streaming them.") 

    Path(output).mkdir(exist_ok=True, parents=True)
    if tempdir is not None:
        Path(tempdir).mkdir(exist_ok=True, parents=True)
    if stream:
        stream_str = "stream and create"
        fs = None
        if try_ros3:
            print("Will attempt to use the ROS3 driver. Ensure that hdf5>=1.12 and h5py>=3.2 are installed from conda-forge.")
        else:
            import fsspec
            from fsspec.implementations.cached import CachingFileSystem
            cache_store = "nwb-cache"
            fs = CachingFileSystem(
                fs=fsspec.filesystem("http"),
                cache_storage=cache_store,  # Local folder for the cache
            )
    else:
        stream_str = "download and replace"
        print(f"Temporary directory: {tempdir}")
        
    asset_urls, asset_content_urls, asset_filenames = get_dandiset_asset_info(part=part)
    print(f"Found {len(asset_urls)} files to {stream_str}.")
        
    for a, (asset_url, asset_content_url, asset_filename) in enumerate(zip(asset_urls, asset_content_urls, asset_filenames)):
        print(asset_filename)
        n_str = f"[{a+1}/{len(asset_urls)}] "
        start = time.perf_counter()
        if stream:
            stream_and_save(asset_content_url, asset_filename, output=output, compress=compress, fs=fs, n_str=n_str)
        else:
            download_and_replace(asset_url, asset_filename, output=output, compress=compress, tempdir=tempdir, n_str=n_str)
        end = time.perf_counter()
        duration = end - start
        minutes = int(duration / 60)
        print(f"    {minutes:02}m {duration - minutes * 60:05.2f}s")
    
    if stream and fs is not None:
        Path(cache_store).rmdir()
    
    overall_end = time.perf_counter()
    duration_min = (overall_end - overall_start) // 60
    print(f"TOTAL: {duration_min} min")
    
            
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--output", default="Plitt2021", type=Path)
    parser.add_argument("--compress", action="store_true")
    parser.add_argument("--tempdir", default=None)
    parser.add_argument("--no_stream", action="store_true")
    parser.add_argument("--part", default="")
        
    parser.add_argument("--try_ros3", action="store_true")
    
    args = parser.parse_args()
    
    # Ignore superfluous warning
    warnings.filterwarnings("ignore", category=UserWarning, message="Ignoring cached namespace")
    
    main(output=args.output, compress=args.compress, tempdir=args.tempdir, stream=not(args.no_stream), try_ros3=args.try_ros3, part=args.part)
