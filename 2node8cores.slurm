#!/bin/bash
#SBATCH --partition=physical
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --time=0:10:00

echo "2 node 8 cores"
module purge
module load foss/2019b
module load python/3.7.4
time srun -n 8 python3 run.py /home/bingx1/comp90024ass1/data/bigTwitter.json
