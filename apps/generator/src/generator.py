import io
from typing import IO, Callable

import pandas as pd
from pandas import DataFrame

from . import storage
from .utils import Report


def gen(report: Report) -> str:
    file_id = report.id
    if report.state != 'complete':
        file_id += '_tmp'

    df = pd.DataFrame(report.rows_for_pd)

    output: IO = get_doc_generator(report)(df)

    storage.save(file_id, output)

    return file_id


def get_doc_generator(report: Report) -> Callable[[DataFrame], IO]:
    return {
        'wb': {
            'fin_monthly': lambda data_frame: wb_fin_doc(data_frame)
        }
    }[report.platform][report.doc_type]


# TODO need debug and correct
def wb_fin_doc(df: DataFrame) -> IO:
    df = df.groupby(
        ['nm_id', 'barcode', 'sa_name', 'name', 'realizationreport_id']
    ).apply(lambda x: pd.Series(dict(
        delivery=(x.delivery_rub.where(x.supplier_oper_name == 'Логистика')).sum(),
        income=(x.supplier_reward.where(x.supplier_oper_name == 'Продажа')).sum(),
        income_number=(x.quantity.where(x.supplier_oper_name == 'Продажа')).sum(),
        refund=(x.supplier_reward.where(x.supplier_oper_name == 'Возврат')).sum(),
        refund_number=(x.quantity.where(x.supplier_oper_name == 'Возврат')).sum(),
    )))

    output = io.BytesIO()

    # Use a temp filename to keep pandas happy.
    writer = pd.ExcelWriter('temp.xlsx', engine='xlsxwriter')

    # Set the filename/file handle in the xlsxwriter.workbook object.
    writer.book.filename = output

    # Write the data frame to the StringIO object.
    df.to_excel(writer, sheet_name='Sheet1')
    writer.save()

    return output
