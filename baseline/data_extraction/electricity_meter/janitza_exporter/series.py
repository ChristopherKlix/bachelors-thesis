from enum import Enum


class SERIES(Enum):
    ENERGY_SUPPLIED = 'energy_supplied_kwh'
    ENERGY_CONSUMED = 'energy_consumed_kwh'
    REACTIVE_ENERGY = 'reactive_energy_kvarh'
    ALL = 'all'
