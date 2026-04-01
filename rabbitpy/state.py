"""Common base class for objects that need to maintain state."""

import typing


class StatefulBase:
    """Base class for classes that need to maintain state such as
    connection and channel.

    """

    CLOSED = 0
    CLOSING = 1
    OPEN = 2
    OPENING = 3

    STATES: typing.ClassVar[dict[int, str]] = {
        0: 'Closed',
        1: 'Closing',
        2: 'Open',
        3: 'Opening',
    }

    def __init__(self, *args: object, **kwargs: object) -> None:
        """Create a new instance of the object defaulting to a closed state."""
        self._state: int = self.CLOSED
        super().__init__(*args, **kwargs)

    def _set_state(self, value: int) -> None:
        """Set the state to the specified value, validating it is a supported
        state value.

        :param value: The new state value

        """
        if value not in self.STATES.keys():
            raise ValueError(f'Invalid state value: {value}')
        self._state = value

    @property
    def is_closed(self) -> bool:
        """Returns True if in the CLOSED runtime state"""
        return self._state == self.CLOSED

    @property
    def is_closing(self) -> bool:
        """Returns True if in the CLOSING runtime state"""
        return self._state == self.CLOSING

    @property
    def is_open(self) -> bool:
        """Returns True if in the OPEN runtime state"""
        return self._state == self.OPEN

    @property
    def is_opening(self) -> bool:
        """Returns True if in the OPENING runtime state"""
        return self._state == self.OPENING

    @property
    def state(self) -> int:
        """Return the runtime state value"""
        return self._state

    @property
    def state_description(self) -> str:
        """Returns the text based description of the runtime state"""
        return self.STATES[self._state]
