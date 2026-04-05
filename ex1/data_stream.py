# ************************************************************************* #
#                                                                           #
#                                                      :::      ::::::::    #
#  data_stream.py                                    :+:      :+:    :+:    #
#                                                  +:+ +:+         +:+      #
#  By: klucchin <klucchin@student.42.fr>         +#+  +:+       +#+         #
#                                              +#+#+#+#+#+   +#+            #
#  Created: 2026/04/05 20:14:59 by klucchin        #+#    #+#               #
#  Updated: 2026/04/05 20:15:02 by klucchin        ###   ########.fr        #
#                                                                           #
# ************************************************************************* #


from abc import ABC, abstractmethod
from typing import Any, List, Tuple


class DataProcessor(ABC):
    def __init__(self):
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
            self._data.extend(data)
        else:
            self._data.append(data)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        def is_valid_log(d):
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

        def format_log(d):
            return f"{d.get('log_level', '')}: {d.get('log_message', '')}"

        if isinstance(data, list):
            for d in data:
                self._data.append(format_log(d))
        else:
            self._data.append(format_log(data))


class DataStream:
    def __init__(self):
        self._processors: List[DataProcessor] = []
        self._stats: dict[str, int] = {}

    def register_processor(self, proc: DataProcessor) -> None:
        name = type(proc).__name__
        self._processors.append(proc)
        self._stats[name] = 0

    def process_stream(self, stream: List[Any]) -> None:
        for element in stream:
            handled = False

            for proc in self._processors:
                if proc.validate(element):
                    proc.ingest(element)

                    name = type(proc).__name__
                    if isinstance(element, list):
                        self._stats[name] += len(element)
                    else:
                        self._stats[name] += 1

                    handled = True
                    break

            if not handled:
                print(
                    "DataStream error - "
                    f"Can't process element in stream: {element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")

        if not self._processors:
            print("No processor found, no data")
            return

        for proc in self._processors:
            name = type(proc).__name__
            total = self._stats[name]
            remaining = len(proc._data)

            print(f"{name.replace('Processor', ' Processor')}: total {total} "
                  f"items processed, remaining {remaining} on processor"
                  )


def main() -> None:
    print("=== Code Nexus - Data Stream ===\n")

    print("Initializing Data Stream...")
    data = DataStream()
    data.print_processors_stats()

    print("\nRegistering Numeric Processor\n")
    num = NumericProcessor()
    data.register_processor(num)
    stream = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {'log_level': 'WARNING', 'log_message':
             'Telnet access! Use ssh instead'},
            {'log_level': 'INFO', 'log_message': 'User wil is connected'}
        ],
        42,
        ["Hi", "five"]
    ]

    print("Send first batch of data on stream:", stream)
    data.process_stream(stream)
    data.print_processors_stats()

    print("\nRegistering other data processors")
    string = TextProcessor()
    log = LogProcessor()
    data.register_processor(string)
    data.register_processor(log)
    print("Send the same batch of data again")
    data.process_stream(stream)
    data.print_processors_stats()

    print("\nConsume some elements from the data processor:"
          " Numeric 3, Text 2, Log 1"
          )
    for i in range(3):
        num.output()
    for i in range(2):
        string.output()
    for i in range(1):
        log.output()

    data.print_processors_stats()


if __name__ == "__main__":
    main()
