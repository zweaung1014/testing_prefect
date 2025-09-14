from prefect import flow, serve
import time

@flow(log_prints=True) # shows in console AND Prefect UI logs
def before_extraction():
    print("Running: weigh_extraction_vial()")
    print("Running: weigh_hplc_vial A1")
    print("Running: weigh_hplc_vial B1")
    print("Running: salsa_liquid_transfer")
    print("Running: weigh_extraction_vial")
    print("Running: extraction_vial_to_reactor")
    print("Running: run_extraction")
    time.sleep(5)

@flow(log_prints=True)
def after_extraction():
    print("Running: after extraction stuff...")
    time.sleep(5)

if __name__ == "__main__":
    serve(
        before_extraction.serve(name="deployment_before_extraction"),
        after_extraction.serve(name="deployment_after_extraction"),
    )
	
    # def JZ_01_09_1_prep():
# 	deck.deck.initialize_deck(**{'folder_path': './'})


# def JZ_01_09_1():
# 	JZ_09_1_crude = deck.deck.weigh_extraction_vial(**{'vial_loc': 'A1'})
# 	JZ_09t_1_empty = deck.deck.weigh_hplc_vial(**{'vial_loc': 'A1'})
# 	JZ_09b_1_empty = deck.deck.weigh_hplc_vial(**{'vial_loc': 'B1'})
# 	deck.deck.salsa_liquid_transfer(**{'destination': 'extract_stock', 'destination_loc': 'A1', 'distance': None, 'source': 'solv_library', 'source_loc': 'C2', 'volume': 4.0})
# 	JZ_09_1_aq = deck.deck.weigh_extraction_vial(**{'vial_loc': 'A1'})
# 	deck.deck.extraction_vial_to_reactor(**{'vial_loc': 'A1'})
# 	JZ_09_1 = deck.deck.run_extraction(**{'output_file': 'JZ-09-1', 'rate': 1000, 'settle_time': 60.0, 'stir_time': 60.0})
# 	top = deck.deck.salsa_liquid_transfer(**{'destination': 'hplc_stock', 'destination_loc': 'A1', 'distance': 10.0, 'source': 'em_vial', 'source_loc': 1, 'volume': 0.1})
# 	bottom = deck.deck.salsa_liquid_transfer(**{'destination': 'hplc_stock', 'destination_loc': 'B1', 'distance': None, 'source': 'em_vial', 'source_loc': 1, 'volume': 0.1})
# 	JZ_09t_1_sample = deck.deck.weigh_hplc_vial(**{'vial_loc': 'A1'})
# 	JZ_09b_1_sample = deck.deck.weigh_hplc_vial(**{'vial_loc': 'B1'})
# 	top = deck.deck.salsa_add_hplc_diluent(**{'vial_loc': 'A1', 'volume': 0.9})
# 	bottom = deck.deck.salsa_add_hplc_diluent(**{'vial_loc': 'B1', 'volume': 0.9})
# 	JZ_09t_1_diluent = deck.deck.weigh_hplc_vial(**{'vial_loc': 'A1'})
# 	JZ_09b_1_diluent = deck.deck.weigh_hplc_vial(**{'vial_loc': 'B1'})
# 	JZ_09t_1 = deck.deck.run_hplc(**{'vial_loc': 'A1'})
# 	JZ_09b_1 = deck.deck.run_hplc(**{'vial_loc': 'B1'})
# 	deck.deck.extraction_vial_from_reactor(**{'vial_loc': 'A1'})
# 	deck.deck.salsa_ph_test(**{'needle_depth': 10, 'source': 'extract_stock', 'source_loc': 'A1'})
# 	return {'JZ_09b_1_sample':JZ_09b_1_sample,'JZ_09t_1':JZ_09t_1,'JZ_09t_1_sample':JZ_09t_1_sample,'JZ_09b_1':JZ_09b_1,'JZ_09b_1_empty':JZ_09b_1_empty,'JZ_09_1_aq':JZ_09_1_aq,'JZ_09t_1_empty':JZ_09t_1_empty,'top':top,'bottom':bottom,'JZ_09t_1_diluent':JZ_09t_1_diluent,'JZ_09_1_crude':JZ_09_1_crude,'JZ_09b_1_diluent':JZ_09b_1_diluent,'JZ_09_1':JZ_09_1,}