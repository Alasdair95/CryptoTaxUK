from apis.get_all_transactions import GetAllTransactions


class CryptoTaxUK:
    def __init__(self):
        self.save_path = 'todo'  # TODO: Fix this

    def execute(self):
        # Generate complete dataset of transactions for each asset
        x = GetAllTransactions()
        x.get_all_transactions()

        # Perform all tax related calculations

        # Generate a final report

        pass


if __name__ == '__main__':
    executor = CryptoTaxUK()
    executor.execute()
