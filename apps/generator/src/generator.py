import io
from typing import Callable, Iterable, Tuple, List

import pandas as pd

from . import storage
from .utils import Report


def gen(report: Report) -> str:
    file_id = report.id
    if report.state != 'complete':
        file_id += '_tmp'

    output: io.BytesIO = io.BytesIO()

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in get_doc_generator(report)(report.rows):
            df.to_excel(writer, sheet_name)
        writer.save()

    output.seek(0, 0)
    storage.save(file_id, output)

    return file_id


def get_doc_generator(report: Report) -> Callable[[List[dict]], Iterable[Tuple[str, pd.DataFrame]]]:
    return {
        'test': {
            'fin_monthly': lambda rows: [('test', pd.DataFrame(rows))]
        },
        'wb': {
            'fin_monthly': lambda rows: wb_fin_doc(rows)
        }
    }[report.platform][report.doc_type]


def wb_fin_doc(rows: List[dict]) -> Iterable[Tuple[str, pd.DataFrame]]:
    def get_rows():
        for row in rows:
            for rep in row['reports']:
                yield {**{key: value for key, value in row.items() if key != 'reports'}, **rep}

    def get_apply(x: pd.DataFrame):
        return pd.Series(dict(
            delivery=(x.delivery_rub.where(x.supplier_oper_name == 'Логистика')).sum(),
            income=(x.supplier_reward.where(x.supplier_oper_name == 'Продажа')).sum(),
            income_number=(x.quantity.where(x.supplier_oper_name == 'Продажа')).sum(),
            refund=(x.supplier_reward.where(x.supplier_oper_name == 'Возврат')).sum(),
            refund_number=(x.quantity.where(x.supplier_oper_name == 'Возврат')).sum(),
        ))

    df: pd.DataFrame = pd.DataFrame(get_rows())

    group_fields = ['nm_id', 'barcode', 'sa_name'] + (['name'] if 'name' in df.columns else [])

    total = df.groupby(group_fields).apply(get_apply)

    yield 'sum', total.sum()
    yield 'total', total

    for _id in df.realizationreport_id.unique():
        yield f'report_{_id}', df.groupby(group_fields).apply(get_apply)
