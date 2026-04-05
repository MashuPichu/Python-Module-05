# ************************************************************************* #
#                                                                           #
#                                                      :::      ::::::::    #
#  data_processor.py                                 :+:      :+:    :+:    #
#                                                  +:+ +:+         +:+      #
#  By: klucchin <klucchin@student.42.fr>         +#+  +:+       +#+         #
#                                              +#+#+#+#+#+   +#+            #
#  Created: 2026/04/04 16:17:06 by klucchin        #+#    #+#               #
#  Updated: 2026/04/05 22:56:02 by klucchin        ###   ########.fr        #
#                                                                           #
# ************************************************************************* #

from typing import Any, List, Tuple
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: List[str] = []

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> Tuple[int, str]:
        if not self._data:
            raise Exception("No data available")
        value = self._data.pop(0)
        return (0, value)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(isinstance(x, (int, float)) for x in data)
        return False

    def ingest(self, data: int | float | List[int | float]) -> None:
        if not self.validate(data):
            raise Exception("Improper numeric data")

        if isinstance(data, list):
            for x in data:
                self._data.append(str(x))
        else:
            self._data.append(str(data))


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(x, str) for x in data)
        return False

    def ingest(self, data: str | List[str]) -> None:
        if not self.validate(data):
            raise Exception("Improper text data")

        if isinstance(data, list):
            for y in data:
                self._data.append(y)
        else:
            self._data.append(data)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        def is_valid_log(d: dict[str, str]) -> bool:
            return (
                isinstance(d, dict)
                and all(isinstance(i, str) for i in d.keys())
                and all(isinstance(j, str) for j in d.values())
            )

        if is_valid_log(data):
            return True
        if isinstance(data, list):
            return all(is_valid_log(d) for d in data)
        return False

    def ingest(self, data: dict[str, str] | List[dict[str, str]]) -> None:
        if not self.validate(data):
            raise Exception("Improper log data")

        def format_log(d: dict[str, str]) -> str:
            return f"{d.get('log_level', '')}: {d.get('log_message', '')}"

        if isinstance(data, list):
            for d in data:
                self._data.append(format_log(d))
        else:
            self._data.append(format_log(data))


def main() -> None:
    print("=== Code Nexus - Data Processor ===\n")

    print("Testing Numeric Processor...")
    num = NumericProcessor()

    print(f"Trying to validate input '42': {num.validate(42)}")
    print(f"Trying to validate input 'Hello': {num.validate('Hello')}")

    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num.ingest('foo')
    except Exception as e:
        print(f"Got exception: {e}")

    print("Processing data: [1, 2, 3, 4, 5]")
    num.ingest([1, 2, 3, 4, 5])
    print("Extracting 3 values...")
    for i in range(3):
        _, value = num.output()
        print(f"Numeric value {i}: {value}")

    print("\nTesting Text Processor...")
    string = TextProcessor()
    print(f"Trying to validate input '42': {string.validate(42)}")

    string.ingest(['Hello', 'Nexus', 'World'])
    print("Extracting 1 value...")
    for i in range(2):
        _, valuestr = string.output()
        print(f"Text value {i}: {valuestr}")

    print("\nTesting Log Processor...")
    log = LogProcessor()

    print(f"Trying to validate input 'Hello': {log.validate('Hello')}")

    data = [
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!"}
    ]

    print(f"Processing data: {data}")
    log.ingest(data)

    print("Extracting 2 values...")
    for i in range(2):
        _, value = log.output()
        print(f"Log entry {i}: {value}")

    print("\nTrying to validate false data...")
    falsedata = [
        {"log_level": 1, "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!"}
    ]
    print(f"Processing data: {falsedata}")
    try:
        log.ingest(falsedata)
        print("Extracting 2 values...")
        for i in range(2):
            _, value = log.output()
            print(f"Log entry {i}: {value}")
    except Exception as e:
        print(f"Got exception: {e}")


if __name__ == "__main__":
    main()
