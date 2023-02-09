#PBS -lwalltime=03:00:00
#PBS -lselect=1:ncpus=1:mem=16gb


# For use on the Imperial HPC

module load anaconda3/personal

source activate basic

# takes a couple of hours
time python $HOME/download_Plitt2021.py --tempdir $TMPDIR/Plitt2021 --output $WORK/Plitt2021
