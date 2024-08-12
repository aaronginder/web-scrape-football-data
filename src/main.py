import luigi
from tasks.tasks import ProcessFootballData


def main():
    """
    Entry function that runs the data pipeline.
    """
    luigi.build(tasks=[ProcessFootballData()], local_scheduler=True)


if __name__ == "__main__":
    main()
