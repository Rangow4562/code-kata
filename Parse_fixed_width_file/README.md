To run with docker (with containerised csv outputs) use
```
#1 : 
docker build -f Dockerfile -t data_processor . --no-cache  

#2 :
docker run --rm -v /path/input:/input -v /path/output:/output -e SPEC_FILE=/input/random_spec.json  -e FWF_FILE=/output/output.fwf  -e CSV_FILE=/output/output.csv -e NUM_LINES=100  data_processor
```

To execute main functions with fixed_width outputs 
```
python fixed_width_parser.py 
```
To execute main functions with csv outputs 
```
python csv_parser.py 
```

To run individual test files use
```
python tests\test_advanced_data_processor.py
python tests\test_data_processor.py
```
The output files will be in `output/*` 


