#include <mpi.h> // Include MPI library
#include <iostream> // Include input/output stream library
#include <fstream> // Include file stream library
#include <vector> // Include vector container library
#include <algorithm> // Include algorithm library for sorting
#include <sstream> // Include string stream library for string manipulation
#include <map> // Include map container library

using namespace std; // Import the std namespace

// Structure for traffic data
struct TrafficSignal {
    int light_id; // ID of the traffic light
    int car_count; // Number of cars at the traffic light
    string time_stamp; // Timestamp of the data
};

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv); // Initialize MPI environment

    int rank, size; // Variables to store MPI rank and size
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); // Get the rank of the current MPI process
    MPI_Comm_size(MPI_COMM_WORLD, &size); // Get the total number of MPI processes

    string filename = "test-traffic-info.txt"; // Filename of the traffic data file

    vector<TrafficSignal> process_data; // Vector to store traffic data
    map<string, vector<TrafficSignal>> grouped_data; // Map to store grouped traffic data by timestamp

    // Master process reads the entire data file
    if (rank == 0) {
        ifstream input_file(filename); // Open the input file
        if (!input_file.is_open()) { // Check if file is opened successfully
            cout << "Error: Could not open file on master process." << endl; // Print error message
            MPI_Finalize(); // Finalize MPI environment
            return 1; // Return with error code
        }

        string line, token; // Variables to store lines and tokens from the file

        getline(input_file, line); // Skip header row

        // Read data line by line from the file
        while (getline(input_file, line)) {
            TrafficSignal signal; // Create a TrafficSignal object
            stringstream ss(line); // Create a stringstream from the line

            // Extract data from the stringstream separated by commas
            getline(ss, token, ',');
            signal.light_id = stoi(token); // Convert light_id to integer

            getline(ss, token, ',');
            signal.time_stamp = token; // Store the timestamp

            getline(ss, token, ',');
            signal.car_count = stoi(token); // Convert car_count to integer

            process_data.push_back(signal); // Add the TrafficSignal object to process_data vector
        }

        input_file.close(); // Close the input file

        // Group data by timestamp
        for (const auto& signal : process_data) {
            grouped_data[signal.time_stamp].push_back(signal); // Group traffic signals by timestamp
        }
    }

    // Broadcast grouped data to all processes
    int num_groups = grouped_data.size(); // Get the number of groups
    MPI_Bcast(&num_groups, 1, MPI_INT, 0, MPI_COMM_WORLD); // Broadcast the number of groups to all processes
    for (const auto& pair : grouped_data) { // Iterate over each group
        int group_size = pair.second.size(); // Get the size of the group
        MPI_Bcast(&group_size, 1, MPI_INT, 0, MPI_COMM_WORLD); // Broadcast the group size to all processes
        for (const auto& signal : pair.second) { // Iterate over each signal in the group
            int light_id = signal.light_id; // Get the light_id
            int car_count = signal.car_count; // Get the car_count
            MPI_Bcast(&light_id, 1, MPI_INT, 0, MPI_COMM_WORLD); // Broadcast the light_id to all processes
            MPI_Bcast(&car_count, 1, MPI_INT, 0, MPI_COMM_WORLD); // Broadcast the car_count to all processes
        }
    }

    // Each process calculates total car count for each traffic light
    vector<int> local_traffic_counts(4, 0); // Vector to store local traffic counts for each light
    for (const auto& pair : grouped_data) { // Iterate over each group
        for (const auto& signal : pair.second) { // Iterate over each signal in the group
            local_traffic_counts[signal.light_id - 1] += signal.car_count; // Accumulate car count for each light
        }
    }

    // Gather local counts from all processes on the master
    vector<int> global_traffic_counts(4 * size, 0); // Vector to store global traffic counts for each light
    MPI_Gather(&local_traffic_counts[0], 4, MPI_INT, &global_traffic_counts[0], 4, MPI_INT, 0, MPI_COMM_WORLD); // Gather local counts from all processes to master

    // Master process identifies the top N congested lights after every hour
    if (rank == 0) {
        int N = 4; // Number of most congested lights to print

        for (const auto& pair : grouped_data) { // Iterate over each group
            vector<pair<int, int>> traffic_lights; // Vector to store traffic lights and car counts
            for (int i = 0; i < size; ++i) { // Iterate over each process
                for (int j = 0; j < 4; ++j) { // Iterate over each traffic light
                    traffic_lights.push_back(make_pair(global_traffic_counts[i * 4 + j], j + 1)); // Store car count and light_id pair
                }
            }

            // Sort traffic lights in descending order of car count
            sort(traffic_lights.rbegin(), traffic_lights.rend());

            // Print the most congested traffic lights
            cout << "Traffic signals arranged on the basis of urgency | Time: " << pair.first << endl;
            cout << "------Traffic Light-------\t\t-----Number of Cars-----" << endl;
            for (int i = 0; i < N && i < traffic_lights.size(); ++i) { // Iterate over top N congested lights
                cout << "\t" << traffic_lights[i].second << "\t\t\t\t\t" << traffic_lights[i].first << endl; // Print light_id and car count
            }
            cout << endl; // Print newline for clarity
        }
    }

    MPI_Finalize(); // Finalize MPI environment
    return 0; // Return with success code
}
