import sys


class CryptoTaxUK:
    def __init__(self):
        if len(sys.argv) > 1:
            self.path = sys.argv[1]
        else:
            self.path = 'test'

    def execute(self):
        # This function will execute 4 steps:
        # - Take self.path and combine all raw data into one data set of transactions per asset
        # - Perform all tax related calculations
        # - Generate the final tax report
        # - Save the final tax report and notify the user where it has been saved
        pass


if __name__ == '__main__':
    executor = CryptoTaxUK()
    executor.execute()
