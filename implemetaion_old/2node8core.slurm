#!/bin/bash
#SBATCH --partition=physical
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --time 1-00:00:00
module purge
module load foss/2019b
module load python/3.7.4
echo "2 node and 8 core"
time srun -n 8 python3 main.py