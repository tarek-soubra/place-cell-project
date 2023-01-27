#!/usr/bin/env python


import argparse
import glob
from pathlib import Path
import shutil
import time

import h5py
from hdmf.backends.hdf5.h5_utils import H5DataIO
from dandi import dandiapi
from dandi import download as dandi_download
import pynwb
from pynwb.ophys import Fluorescence, RoiResponseSeries



# get URLs and file names
def get_dandiset_asset_info(dandiset_id="000054", version="draft", part=""):
    
    client = dandiapi.DandiAPIClient()
    dandi = client.get_dandiset(dandiset_id, version)
    
    asset_urls, asset_filenames = list(zip(*[
        [asset.download_url, Path(asset.path).name] for asset in dandi.get_assets()
    ]))
    
    sort_order = [asset_filenames.index(name) for name in sorted(asset_filenames) if part in name]
    asset_urls = [asset_urls[i] for i in sort_order]
    asset_filenames = [asset_filenames[i] for i in sort_order]
        
    return asset_urls, asset_filenames


# pop out acquisition module (in place) (e.g., 28 GB)
def pop_acquisition(read_nwbfile):
    acquisition = read_nwbfile.acquisition.pop("TwoPhotonSeries")
    
    
# compress ROI data (in place) (e.g., 300 MB)
def compress_roi_data(read_nwbfile):
    fluo = read_nwbfile.processing["ophys"].data_interfaces.pop("Fluorescence")    
    dff_interface = Fluorescence(name="Fluorescence")
    read_nwbfile.processing["ophys"].add(dff_interface)
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
        dff_interface.add_roi_response_series(new_series)


def create_new_file(load_info, filename, compress=False, output=None):
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

    
def stream_and_save(asset_url, filename, output=".", compress=False, n_str=""):
    Path(output).mkdir(exist_ok=True, parents=True)
    new_filename = Path(output, filename)
    
    print(f"{n_str}File: {filename}:")
    existing_files = glob.glob(str(Path(output, "*.nwb")))
    stem = Path(filename).stem
    if sum([stem in existing_file for existing_file in existing_files]):
        print("    Skipping. File already exists.")
        return
    
    import fsspec
    from fsspec.implementations.cached import CachingFileSystem
    
    # first, create a virtual filesystem based on the http protocol and use
    # caching to save accessed data to RAM.
    cache_storage = Path(output, "nwb-cache")
    fs = CachingFileSystem(
        fs=fsspec.filesystem("http"),
        cache_storage=str(cache_storage),  # Local folder for the cache
    )

    with fs.open(asset_url, "rb") as f:
        with h5py.File(f) as h5_handle:
            load_info = {"file": h5_handle, "load_namespaces": True}
            create_new_file(load_info, new_filename, compress=compress, output=output)
    
    shutil.rmtree(cache_storage)
    


def main(output=".", compress=False, tempdir=None, stream=False, part=""):

    if not stream:
        print("Warning: This will be take much longer (e.g., 10x) if downloading the full data files, instead of streaming them.") 

    Path(output).mkdir(exist_ok=True, parents=True)
    if tempdir is not None:
        Path(tempdir).mkdir(exist_ok=True, parents=True)
    if stream:
        stream_str = "stream and create"
    else:
        stream_str = "download and replace"
        print(f"Temporary directory: {tempdir}")
    asset_urls, asset_filenames = get_dandiset_asset_info(part=part)
    print(f"Found {len(asset_urls)} files to {stream_str}.")
        
    for a, (asset_url, asset_filename) in enumerate(zip(asset_urls, asset_filenames)):
        n_str = f"[{a+1}/{len(asset_urls)}] "
        start = time.time()
        if stream:
            stream_and_save(asset_url, asset_filename, output=output, compress=compress, n_str=n_str)
        else:
            download_and_replace(asset_url, asset_filename, output=output, compress=compress, tempdir=tempdir, n_str=n_str)
        end = time.time()
        duration = end - start
        minutes = int(duration / 60)
        print(f"    {minutes:02}m {duration - minutes * 60:05.2f}s")
            
            
            
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--output", default="Plitt2021", type=Path)
    parser.add_argument("--compress", action="store_true")
    parser.add_argument("--tempdir", default=None)
    parser.add_argument("--no_stream", action="store_true")
    parser.add_argument("--part", default="")
    
    args = parser.parse_args()
    
    main(output=args.output, compress=args.compress, tempdir=args.tempdir, stream=not(args.no_stream), part=args.part)
