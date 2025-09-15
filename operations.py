import datetime
import time
import os

import numpy as np

from automated_lle.components.logger.logger import logger, file_log
from automated_lle.components.abstract_deck import AbstractDeck
from automated_lle.implementations.SDL7.sdl7_ur_manager import ArmManager
from typing import Union, List, Optional
from automated_lle.resources.containers import BaseContainer, Hplc
from datetime import datetime
from collections import deque


class Operations:

    def __init__(self, deck= None):
        self.arm = deck.arm
        self.deck = deck
        self.hplc_queue = deque()

    @property
    def logger(self):
        return self.deck.logger

    @property
    def trays(self):
        return self.deck.trays

    def initialize_deck(self, experiment_name: str = None, solvent_file: str = None):
        """
        Initializes the deck by creating a new directory for the experiment data.
        :param experiment_name(str): indicates the name of the experiment
        :type experiment_name: str
        :param solvent_file(str): indicates the solvents to be used in the workflow
        """

        if solvent_file:
            if solvent_file.endswith('.json'):
                pass
            else:
                solvent_file += '.json'

            self.trays['solvent'].solvent_info_from_file(solvent_file)

        if not experiment_name:
            experiment_name = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        self.deck.initialize_experiment(experiment_name=experiment_name)

        self.logger.debug(f"Saving container tray summaries in {self.deck.vial_dir}")

        self.deck.initialize_instruments(experiment_name=experiment_name, solvent_file=solvent_file)


    def hold(self, time_min: float):
        """
        holds the current action for the specified time
        :param time_min(float): indicates the amount of minutes to hold the action
        """
        self.logger.info(f'Holding for {time_min} min')
        time.sleep(time_min * 60)

    def heat(self, initial_temp: float = None, final_temp: float = None, time_min: int = 1):
        """
        teruns on heating on the shaker
        :param initial_temp(float): starting temperature (in °C)
        :param final_temp(float): final temperature (in °C)
        :param time_min(int): time span for heating/cooling (in min)

        """

        if initial_temp is None:
            self.deck.temperature_hold(temp=final_temp)
            time.sleep(time_min * 60)
            self.logger.info(f'Set temperature at {final_temp} C')
        else:
            self.deck.temperature_ramp(start_temp=initial_temp, end_temp=final_temp, time_min=time_min)
            self.logger.info(f'Heating from {initial_temp}to {final_temp} C over {time_min} min')

    def set_stirring(self, rate: int = 1000, reactor: int = 1, on: bool = True):
        """
        Sets the stirring rate for the specified reactor.
        :param rate: stirring rate in rpm
        :param reactor: reactor number (1 or 2)
        :param on: if True turns stirring on, if False it stops the stirring
        """
        if reactor not in [1, 2]:
            raise ValueError("Reactor must be 1 or 2")
        if not on:
            self.deck.stop_stirring(reactor=reactor)
            self.logger.info(f'Stopped stirring in reactor {reactor}')
            return
        else:
            self.deck.start_stirring(reactor=reactor, rate=rate)
            self.logger.info(f'Started stirring at {rate} rpm in reactor {reactor}')

    def move_container(self, source_index: str = None, source_container: str = None,
                       destination_index: str = None, destination_container: str = None):

        """
        moves container from a starting tray to destination tray
        :param source_index(str): index of starting point
        :param source_tray(str): type of starting tray
        :param destination_index(str): index of destination tray
        :param destination_tray(str): type of destination tray
        """

        if source_container in ['reactor', 'easymax']:
            self.deck.extraction_vial_from_reactor()
        elif source_container == 'hplc_instrument':
            self.arm.vial_from_hplc_instrument()
            self.arm.to_exchange_horizontal()
            self.deck.hplc.has_vial = False
            self.deck.hplc.vial = None
        else:
            self.arm.container_from_tray(index=source_index, tray=source_container)
        # Decide where the container goes
        if destination_container in ['reactor', 'easymax']:
            self.deck.extraction_vial_to_reactor(index=source_index)
        elif destination_container == 'hplc_instrument':
            self.arm.to_exchange_vertical()
            self.arm.vial_to_hplc_instrument()
            self.deck.hplc.has_vial = True
            self.deck.hplc.vial = self.trays['hplc'][source_index]
        else:
            self.arm.container_to_tray(index=destination_index, tray=destination_container)

    def run_extraction(self, stir_time: Union[int, float] = 1, settle_time: Union[int, float] = 1,
                       rate: int = 1000, reactor: int = 1, time_units: str = "min", temperature: float = None):
        """
        Runs an extraction with indicated stirring and settling times.
        :param stir_time: time for stirring (in min or sec)
        :param settle_time: time for settling (in min or sec)
        :param rate: stirring rate (in rpm)
        :param reactor: reactor number (1 or 2)
        :param time_units: time units for stir_time and settle_time ("min" or "sec")
        :param temperature: temperature to set the reactor to (in °C), if None, uses current temperature
        """

        if time_units == "min":
            stir_time = stir_time * 60
            settle_time = settle_time * 60
        elif time_units == "sec":
            stir_time = stir_time
            settle_time = settle_time
        else:
            raise ValueError("time_units must be 'min' or 'sec'")
        
        if temperature:
            self.deck.temperature_hold(temp=temperature, reactor=reactor)
            self.logger.info(f"Setting reactor {reactor} temperature to {temperature} C")

        reactor_temp = self.deck.get_reactor_temperature(reactor=reactor)
        self.logger.info(f"Extraction unit reactor temperature is {reactor_temp} C")
        self.deck.start_recording()
        try:
            self.set_stirring(rate=rate, reactor=reactor, on=True)
            time.sleep(stir_time)
            self.set_stirring(rate=rate, reactor=reactor, on=False)
            time.sleep(settle_time)
        finally:
            self.deck.stop_recording()
            self.deck.easymax.vial.add_video_files("extraction", self.deck.camera.videos_recorded[-1])

    def weigh_container(self, container_type: str, index: str = None, sample_name: str = None, to_hplc_inst: bool = False ):
        """
        Weighs the specified container and logs the weight.
        :param index: vial identifier (e.g., "A1")
        :param container_type: tray identifier (e.g., "extraction")
        :param sample_name: name of the sample being weighed
        """
        if index is None:
            vial = self.trays[container_type].get_next_available()
        else:
            vial = self.trays[container_type][index]

        weight = self.deck.weigh_container(container_type=container_type, index=vial.well_name, to_hplc_inst=to_hplc_inst)

        vial.add_weight_measurement(sample_name, weight)

    def charge_liquid(self, volume_ml: float, source_container: str = None, source_index: str = None,
                      destination_container: str = None,
                      destination_index: str = None, aspirate_speed_mm_s: int = None,
                      dispense_speed_mm_s: int = None, clean: bool = True, clean_volume: int= 900):

        """
        adds liquid to the container (optional from given container)
        :param volume_ml : volume to be added (in ml)
        :param source_container: type of the container where liquids is sourced
        :param source_index: the index of the container the liquid is sourced from
        :param destination_container: type of container to transfer the liquid to
        :param destination_index: the index of the container where the liquid is charged
        """

        if destination_index is None:
            vial = self.trays[destination_container].get_next_available()
        else:
            vial = self.trays[destination_container][destination_index]

        self.arm.mlh_pickup()
        self.deck.transfer_liquid(volume_ml=volume_ml, source_index=source_index,
                                  source_container=source_container,
                                  destination_index=vial.well_name, destination_container=destination_container,
                                  aspirate_speed_mm_s=aspirate_speed_mm_s,
                                  dispense_speed_mm_s=dispense_speed_mm_s)
        self.arm.mlh_return()
        if clean:
            self.deck.mlh.clean_needle(clean_volume=clean_volume)

        if source_container in ['backend', 'backing']:
            content = self.deck.mlh.backend.well_name
            source_index= self.deck.mlh.backend.well_name
        else:
            content = f"{source_container}-{source_index}"

        vial.add_content(content, volume_ml)

        self.logger.info(f'Charged {volume_ml} ml liquid from {source_container}-{source_index}'
                         f' to {vial.tray_name}-{vial.well_name}')

    def sample(self, sample_volume_ul: float, diluent_volume_ml: float, source_index: str,
               source_container: str, gravimetric: bool = True, vial_index: Hplc= None):
        """
        prepares hplc sample
        (weighs empty hplc vial container, adds liquid from stock container to hplc vial,
        weighs the hplc vial again, adds diluent to hplc vial)
        :param sample_volume_ul(float): volume taken from the stock for hplc (in uL)
        :param diluent_volume_ml(float): volume of diluent added to hplc vial (in mL)
        :param source_index(str): index of the vial in source tray
        :param source_container(str): type of the source container
        :param gravimetric(bool): indicates if we want to weigh the container before and after

        """
        # assuming destination is always hplc vial
        if not vial_index:
            hplc_vial = self.trays['hplc'].get_next_available()
        else:
            hplc_vial= self.trays['hplc'][vial_index]

        destination_container = hplc_vial.tray_name
        destination_index = hplc_vial.well_name

        if source_container in ['easymax', 'reactor']:
            source = self.deck.easymax.vial
        else:
            source = self.trays[source_container][source_index]

        if gravimetric:
            self.weigh_container(container_type='hplc', index=destination_index, sample_name='empty')

        self.charge_liquid(volume_ml=sample_volume_ul / 1000, source_container=source_container,
                           source_index=source_index, destination_container=destination_container,
                           destination_index=destination_index, clean=False)

        if gravimetric:
            self.weigh_container(container_type='hplc', index=destination_index,
                                 sample_name=f'{source_container}_{source_index}')

        # add diluent to the hplc vial
        self.charge_liquid(volume_ml=diluent_volume_ml, source_container='backend',
                           source_index='backend', destination_container=destination_container,
                           destination_index=destination_index)

        if gravimetric:
            self.weigh_container(container_type='hplc', index=destination_index,
                                 sample_name=self.deck.mlh.backend.well_name, to_hplc_inst=True)

        ### only for updating the container

        self.trays[destination_container][destination_index].sampled_from = \
            f"{source.tray_name}_{source.well_name}"
        self.trays[destination_container][destination_index].add_content(f'{source_container}_{source_index}', sample_volume_ul / 1000)
        self.trays[destination_container][destination_index].add_content(self.deck.mlh.backend.well_name, diluent_volume_ml)

        # add the container to the hplc queue
        self.hplc_queue.append(self.trays[destination_container][destination_index])

        self.logger.info(f'Sampling {sample_volume_ul} uL from {source_container}-{source_index} for HPLC analysis')
        self.logger.info(f'Adding {diluent_volume_ml} mL of backend solvent to the HPLC vial')
   
    def prepare_extraction_vial(self, extraction_vial: str = None,
                                crude_container: str = None, crude_index: str = None,
                                crude_volume_ml: Union[float, int] = 3,
                                org_container: str = None, org_index: str = None,
                                org_volume_ml: Union[float, int] = 2,
                                aq_container: str = None, aq_index: str = None,
                                aq_volume_ml: Union[float, int] = 2):

        tray = 'extraction'
        if extraction_vial is None:
            v = self.trays[tray].get_next_available()
        else:
            if isinstance(extraction_vial, str):
                v = self.trays[tray][extraction_vial]
            else:
                v= extraction_vial

        self.weigh_container(index=v.well_name, container_type=tray, sample_name="empty")

        if crude_container:
            self.charge_liquid(volume_ml=crude_volume_ml, source_container=crude_container,
                               source_index=crude_index, destination_container=v.tray_name,
                               destination_index=v.well_name)
            self.weigh_container(index=v.well_name, container_type=tray, sample_name="crude")

        if org_container:
            self.charge_liquid(volume_ml=org_volume_ml, source_container=org_container,
                               source_index=org_index, destination_container=v.tray_name,
                               destination_index=v.well_name)
            organic_solvent= f"{org_container}-{org_index}"
            self.weigh_container(index=v.well_name, container_type=tray, sample_name=organic_solvent)

        if aq_container:
            self.charge_liquid(volume_ml=aq_volume_ml, source_container=aq_container,
                               source_index=aq_index, destination_container=v.tray_name,
                               destination_index=v.well_name)
            aqueous_solvent = f"{aq_container}-{aq_index}"
            self.weigh_container(index=v.well_name, container_type=tray, sample_name=aqueous_solvent)

    def monitor_extraction(self, stir_time: int = 1, settle_time: int = 2, rate: int = 1000,
                           output_file: str = "extraction_data.csv"):
        """
        Run the extraction process with stirring and settling times.
        """
        ## run_extraction()
        ## add decision making point
            # check # of layers
            # based on box height, estiamte volume of each layer
        ## if 2 layers, sample + hplc
        ## if 1 layer, do something or remove
    
    def run_hplc(self, method_name: str = None, inj_vol: int = 2, sample_name: str = None,
                 vial: Optional[Hplc] = None, stall:bool = True):
        """
        runs hplc
        :param method_name(str): hplc method used
        :param sample_name(str): name of the sample
        :param inj_vol(int): volume of injection (ul)
        :param vial(Optional[Hplc]: vial type to run hplc
        :param stall(bool): wait for hplc execution to finish (enables async execution)

        """
        self.deck.run_hplc(method=method_name, inj_vol=inj_vol, sample_name=sample_name, vial=vial, stall=stall)

        if not vial:
            vial = self.deck.hplc.vial

        vial.add_lc_data_directory(self.deck.hplc.data_directory[-1])
        vial.update_lc_instrument_parameters(inj_vol, method_name, self.deck.hplc.SAMPLE_LOCATION)

        self.logger.info(f'Running HPLC on vial {vial.tray_name}-{vial.well_name}')

    def retrieve_lc_data(self, wavelength:int, hplc_vial, channel:str =None, peaks:bool =True ):
        """
        retrieves hplc data
        :param wavelength(int): indicates the wavelength to retrieve the data
        :prarm hplc_vial(Container): name of the hplc vial for retrieving the data
        :param channel(str): indicates the preset wavelengths in ChemStation
        :param peaks(bool): indicated if we want to collect more than one peak
        """
        _, peaks_datafile = self.deck.get_hplc_data(vial=hplc_vial, channel=channel, wavelength=wavelength, peaks=peaks)
        self.logger.info(f'Retrieving HPLC data for vial {hplc_vial.tray_name}-{hplc_vial.well_name} at wavelength {wavelength} nm')
        hplc_vial.add_lc_peaks(peaks_datafile)
    
