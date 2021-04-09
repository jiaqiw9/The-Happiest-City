#!/bin/bash
#SBATCH --partition=physical
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time 1-00:00:00
module purge
module load foss/2019b
module load python/3.7.4
echo "1 node and 1 core"
time srun -n 1 python3 main.py