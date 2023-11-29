

class AssertUtil:

    @staticmethod
    def not_none(obj, message) -> None:
        if obj is None:
            raise ValueError(message)
