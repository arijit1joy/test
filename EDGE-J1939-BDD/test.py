import filecmp
import utilities.file_utility.file_handler as file_utility


compare_ngdi_file_name = "C:/Users/ua519/Desktop/Workspace_new/da_edge_j1939_services/EDGE-J1939-BDD/data/j1939_fc/compare/j1939_fc_ebu_converted_file.json"
downloaded_file = "C:/Users/ua519/Desktop/Workspace_new/da_edge_j1939_services/EDGE-J1939-BDD/data/j1939_fc/download/received_j1939_fc_ebu_converted_file.json"

print(file_utility.same_file_contents(compare_ngdi_file_name, downloaded_file))
