# Use an official Python runtime as a parent image
FROM python:3.10-slim

WORKDIR /app

COPY . .

ENV PYTHONPATH "${PYTHONPATH}:/app"

CMD python3.10 fixed_width_parser.py --spec_file $SPEC_FILE --fwf_file $FWF_FILE -n $NUM_LINES && \
    python3.10 csv_parser.py --spec_file $SPEC_FILE --fwf_file $FWF_FILE --csv_file $CSV_FILE
