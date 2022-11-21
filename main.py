import time

from pandas import DataFrame, read_csv
import numpy as np
from itertools import accumulate


def time_decorator(func):
    def new_func(*args, **kwargs):
        now_time = time.time()
        res = func(*args, **kwargs)
        print(f"function `{func.__name__}` work time = {time.time() - now_time} sec")
        return res
    return new_func


@time_decorator
def read_data_file(filename: str) -> DataFrame:
    return read_csv(filename)


@time_decorator
def add_session_id_column(data_frame: DataFrame):
    """
    Добавляет столбец `session_id` в DataFrame. Оставляет исходный порядок строк в DataFrame.
    """
    data = np.lexsort((data_frame["timestamp"], data_frame["customer_id"]))
    is_equal_previous_array = data_frame.customer_id[data].shift() == data_frame.customer_id[data]
    is_less_previous_array = data_frame.timestamp[data] - data_frame.timestamp[data].shift() <= 3 * 60
    previous_equal_count = 0 + (is_equal_previous_array & is_less_previous_array)
    subtract_into_index = np.array(list(accumulate(
        previous_equal_count,
        lambda x, y: x - y if y == 1 else x
    )))
    result_array = np.sum([subtract_into_index, np.arange(len(data))], axis=0)
    data_frame.loc[data, "session_id"] = result_array
    data_frame["session_id"] = data_frame["session_id"].astype(int)


a: DataFrame = read_data_file("data.csv")
add_session_id_column(a)
