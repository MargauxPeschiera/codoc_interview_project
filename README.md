# codoc_interview_project

Data-engineer interview project for Codoc
: 2 exercices explained into [exercices.pdf](exercices.pdf)

## How to use the git repository 

1. Download the git repository 

2. Install dependencies, "uncommon" libraries used are : 
    - sqlite3
    - datefinder 
    - docx2txt
    - tika
    
3. Go to codoc_interview_project folder 
4. Execute `python database.py` 

## The files 

- [database.py](database.py) : containing the database related function and inserting the data into ([drwh.db](drwh.db))
- [reading.py](reading.py) : containing functions to process excel file 
- [document.py](document.py) : containing functions to process docx and pdf files
- fichiers source : folder containing the excel, docx and pdf files to process
