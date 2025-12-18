from dw import DW
import extract
import transform
import load


if __name__ == '__main__':
    dw = DW(create=True)

    load.load(dw,
        transform.transform(
            extract.extract(), apply_business_rules=True
        )
    )

    dw.close()
