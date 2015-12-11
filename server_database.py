__author__ = 'multiangle'

"""
    NAME:       server_database
    PY_VERSION: python3.4
    FUNCTION:   This file deal with the event of databases.
                The several main task is:
                1. get data in cache table and insert to main table and make sure that
                data in main table is unique. When a new data inserted to main table,
                the same user in ready_to_get table should be deleted
                2.
    VERSION:    _0.1_

    UPDATE_HISTORY:
        _0.1_:  The 1st edition
"""
#TODO 需求文档未完成