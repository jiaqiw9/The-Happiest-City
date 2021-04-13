#!/bin/bash
#SBATCH --nodes=1
#SBATCH --time=0:10:00
#SBATCH --ntasks-per-node=8
#SBATCH --partition=physical

echo "1 Node 8 Cores"
module purge
module load foss/2019b
module load python/3.7.4
time srun -n 8 python3 run.py /home/bingx1/comp90024ass1/data/bigTwitter.json
