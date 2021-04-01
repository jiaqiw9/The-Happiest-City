#!/bin/bash
#SBATCH --partition=physical
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
module purge
module load foss/2019b
module load python/3.7.4
time srun -n 8 python3 main.py