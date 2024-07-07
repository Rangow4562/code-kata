To run with docker (with containerised csv outputs) use
```
docker build -f Dockerfile -t anonymizer . --no-cahce
docker run anonymizer
```

To execute main functions with csv outputs 
```
python main.py 
```

The output files will be in `output/*` 


To run individual test files use
```
python test_anonymizer.py
python test_generator.py
```
To run with docker (with containerised pyspark csv outputs) use:

docker build -f Dockerfile-hadoop -t anonymizer-hadoop . --no-cahce
docker run anonymizer-hadoop
