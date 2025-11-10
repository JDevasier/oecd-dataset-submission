# MegaTabFact: A Dataset of Massive Tables for Automatic Fact-Checking

## Claims Dataset
This repository contains the test set and a small sample of the training set. 

The full train and test datasets can be downloaded from [Huggingface](https://huggingface.co/datasets/jdevasier/MegaTabFact): `jdevasier/MegaTabFact`. 

## OECD Database
We have provided two scripts to help create the OECD database. `download_datasets.py` can be used to download all of the OECD statistics data files. `csv_to_db.py` can then be used to convert all of the CSV files to a database. 

We are currently exploring options for hosting our preprocessed database to make the setup process smoother. 
