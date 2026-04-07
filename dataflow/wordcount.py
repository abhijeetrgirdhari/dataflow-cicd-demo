import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class SplitWords(beam.DoFn):
    def process(self, element):
        return element.split()

def run():
    options = PipelineOptions(
        runner='DataflowRunner',
        project='sandbox-explorer-490214',
        region='us-central1',
        temp_location='gs://test-bucket-2015/temp1',
        staging_location='gs://test-bucket-2015/staging'
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read File' >> beam.io.ReadFromText('gs://test-bucket-2015/input/sample.txt')
            | 'Split Words' >> beam.ParDo(SplitWords())
            | 'Pair with 1' >> beam.Map(lambda word: (word, 1))
            | 'Group and Count' >> beam.CombinePerKey(sum)
            | 'Format Output' >> beam.Map(lambda x: f"{x[0]}: {x[1]}")
            | 'Write Output' >> beam.io.WriteToText('gs://test-bucket-2015/output/result')
        )

if __name__ == '__main__':
    run()
