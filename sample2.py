import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def run():
    pipeline_options = PipelineOptions(["--runner=DirectRunner"])

    with beam.Pipeline() as pipeline:
        plants = (
            pipeline
            | "Gardening plants"
            >> beam.Create(
                [
                    ["🍓Strawberry", "🥕Carrot", "🍆Eggplant"],
                    ["🍅Tomato", "🥔Potato"],
                ]
            )
            | "Flatten lists" >> beam.FlatMap(lambda elements: elements)
            | beam.Map(print)
        )


if __name__ == "__main__":
    run()
