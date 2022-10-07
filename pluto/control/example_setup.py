# note: this script is called by some sort of launcher it is the main
# entry... (must be executed into a certain environment... controls which accounts)
# can be used etc.


def setup(context):
    # setup function, called by the launcher.

    # todo: we have a different controller depending on the mode we are
    #  live controller downloads data...
    #  note: a data ingestion step must be performed prior to download (from some source)
    #  ingestion is done up until the last close time. Ingestion is done starting from the last successful ingestion
    #  until the last close datetime starting from today.

    capital = context.account.capital

    # add 10 strategies and divide capital between them equally
    for i in range(10):
        stg = context.add_strategy('blitzkrieg')

        # strategy setup.
        stg.capital = capital/10


