from enum import Enum


class VALUES(Enum):
    PowerActive = 'PowerActive'
    ActiveEnergy = 'ActiveEnergy'
    ActiveEnergySupplied = 'ActiveEnergySupplied'
    ActiveEnergyConsumed = 'ActiveEnergyConsumed'
    ReactiveEnergy = 'ReactiveEnergy'
    ReactiveEnergyCap = 'ReactiveEnergyCap'
    ReactiveEnergyInd = 'ReactiveEnergyInd'
    U_Effective = 'U_Effective'
    I_Effective = 'I_Effective'
    Frequency = 'Frequency'
    CosPhi = 'CosPhi'
    TotalHarmonicDisturbation_U = 'TotalHarmonicDisturbation_U'
    TotalHarmonicDisturbation_I = 'TotalHarmonicDisturbation_I'
    Temperature = 'Temperature'
    PowerReactivefund = 'PowerReactivefund'


class TYPES(Enum):
    L1 = 'L1'
    L2 = 'L2'
    L3 = 'L3'
    L4 = 'L4'
    SUM13 = 'SUM13'
    SUM14 = 'SUM14'
    L2L1 = 'L2L1'
    L3L2 = 'L3L2'
    L1L3 = 'L1L3'
    OVERALL = 'Overall'


class TIMEBASES(Enum):
    HOUR = '3600'
    QUARTER_HOUR = '900'


class PROJECTS(Enum):
    UNSET = None
    EP_OM_GridVis = 'EP_OM_GridVis'
