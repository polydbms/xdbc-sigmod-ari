import pandas as pd
import matplotlib.pyplot as plt
import os  # Add this import at the top
import numpy as np
import seaborn as sns

# # ****************************Section1 :Generate figures 12 & 13***************************

# # filenames = ['figureACSVCSV','figureACSVCSVOpt']
# filenames = ['figureACSVCSV','figureACSVCSVOpt','figureBCSVPG','figureBCSVPGOpt']

# output_mapping = {
#     'figureACSVCSV': 'par_scale_csv_csv_write_1.pdf',
#     'figureACSVCSVOpt': 'par_scale_csv_csv_ser_8.pdf',
#     'figureBCSVPG': 'par_scale_postgres_csv_write_1.pdf',
#     'figureBCSVPGOpt': 'par_scale_postgres_csv_read_8.pdf',
# }

# # --- Main Loop ---
# # Process each file specified in the `filenames` list.
# for filename in filenames:
#     # Check if the file exists before trying to load it.
#     csv_file_path = os.path.join('res', f'{filename}.csv')
#     if not os.path.exists(csv_file_path):
#         print(f"Warning: File not found at '{csv_file_path}'. Skipping.")
#         continue

#     # Load the CSV data from the file.
#     df = pd.read_csv(csv_file_path)

#     # To handle any repeated experiments, group by the configuration
#     # and average the time. This is more robust than dropping duplicates.
#     config_cols = [
#         'env', 'read_par', 'deser_par', 'comp_par', 'send_par',
#         'rcv_par', 'decomp_par', 'ser_par', 'write_par', 'table'
#     ]
#     df = df.groupby(config_cols, as_index=False)['time'].mean()

#     # List of parallelism parameters to plot on the x-axis
#     parallelism_keys = [
#         'read_par', 'deser_par', 'comp_par',
#         'decomp_par', 'ser_par', 'write_par'
#     ]

#     # Create a new figure for each file.
#     plt.figure(figsize=(6, 3.75))

#     # The values for the x-axis.
#     scale_values = [1, 2, 4, 8, 16]

#     # Plot each parameter's scaling behavior.
#     for param in parallelism_keys:
#         # --- DYNAMIC FILTERING LOGIC ---
#         # 1. Define the default baseline configuration for each iteration.
#         baseline_config = {
#             'read_par': 1, 'deser_par': 1, 'comp_par': 1,
#             'decomp_par': 1, 'ser_par': 1, 'write_par': 1
#         }

#         # 2. Check for the "Opt" special case and adjust the baseline.
#         # This makes the script adaptable to different experimental setups.
#         if "figureACSVCSVOpt" in filename and param != 'ser_par':
#             baseline_config['ser_par'] = 8
#         if "figureBCSVPGOpt" in filename and param != 'read_par':
#             baseline_config['read_par'] = 8

#         # 3. Filter the DataFrame using the correct dynamic baseline.
#         filtered_df = df.copy()
#         for key, value in baseline_config.items():
#             if key != param:  # Don't filter the parameter we are currently plotting
#                 filtered_df = filtered_df[filtered_df[key] == value]
#         # --- END OF LOGIC ---

#         # Extract the data for the current parameter.
#         filtered_df = filtered_df[filtered_df[param].isin(scale_values)].sort_values(by=param)

#         # Plot the results only if data was found.
#         if not filtered_df.empty:
#             grouped = filtered_df.groupby(param)['time'].mean().reset_index()
#             plt.plot(grouped[param], grouped['time'], marker='x', label=f"{param}")

#     # --- Final Plot Customization ---
#     plt.ylim(bottom=0)
#     plt.xlabel('Parallelism Degree')
#     plt.ylabel('Time (s)')
#     plt.xticks(scale_values)
#     plt.legend(loc='best', ncol=3)
#     plt.grid(alpha=0.3)
#     plt.tight_layout()

#     # Save the plot to the corresponding PDF file.
#     output_filename = output_mapping.get(filename, f'{filename}.pdf')
#     plt.savefig(output_filename, bbox_inches='tight')
#     print(f"Plot saved to '{output_filename}'")

#     plt.show()



# # ******************************Section2: Generate figure for MemoryManagement**************************

#     # Load the CSV file
# filename = "figureMemoryManagement"
# csv_file_path = os.path.join('res', f'{filename}.csv')
# if not os.path.exists(csv_file_path):
#     print(f"Warning: File not found at '{csv_file_path}'. Skipping.")
# data = pd.read_csv(csv_file_path)

# data = data[data['table']=='lineitem_sf10_nocomp']  # Filter for the specific table
# # Prepare plot data
# data['bufferpool_size'] = data['bufferpool_size'].astype(int)
# data['buffer_size'] = data['buffer_size'].astype(int)

# # Sort buffer pool and buffer sizes for consistency
# data = data.sort_values(['bufferpool_size', 'buffer_size'])

# # Set up plot style
# plt.rcParams['text.usetex'] = True
# plt.rcParams['font.family'] = 'serif'
# plt.rcParams['font.serif'] = ['Computer Modern Roman']
# plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})

# # Unique buffer pool sizes and buffer sizes
# bufferpool_sizes = sorted(data['bufferpool_size'].unique())
# buffer_sizes = sorted(data['buffer_size'].unique())
# colors = sns.color_palette("colorblind", len(buffer_sizes))

# # Plot
# plt.figure(figsize=(6, 3.75))
# bar_width = 0.14
# x_indexes = np.arange(len(bufferpool_sizes))

# for i, buffer_size in enumerate(buffer_sizes):
#     subset = data[data['buffer_size'] == buffer_size]
#     means = [
#         subset[subset['bufferpool_size'] == pool_size]['time'].mean() if pool_size in subset['bufferpool_size'].values else 0
#         for pool_size in bufferpool_sizes
#     ]
#     positions = x_indexes + (i - len(buffer_sizes) / 2) * bar_width
#     plt.bar(positions, means, width=bar_width, label=f"{buffer_size}", color=colors[i], zorder=3)

# # Formatting
# plt.xlabel("Buffer Pool Size (MB)")
# plt.ylabel("Time (s)")
# plt.ylim(0,34)
# plt.xticks(ticks=x_indexes, labels=[f"{int(bp/1024)}" for bp in bufferpool_sizes], ha="center")
# plt.legend(title="Buffer Size (KB)",loc='best', ncol=7, labelspacing=0.1, borderpad=0.3, handletextpad=0.2, handlelength=1, columnspacing=.3)
# plt.grid(axis='y', alpha=0.3, zorder=0)

# # Save and show the plot
# plt.tight_layout()
# plt.savefig("bufferpool_vs_buffersize.pdf", bbox_inches='tight')
# plt.show()



# # ******************************Section3: Generate figure for physical nodes**************************
# # Define the environments and approaches
# environments = ['big-big', 'small-big', 'big-laptop', 'small-laptop']
# approaches = ['xdbc[comp]', 'xdbc[nocomp]', 'xdbc[nocomp+skipser]', 'netcat[comp]', 'netcat[nocomp]']

# # Define the hardcoded times for each approach and environment
# times = np.array([
#     [29, 24, 212, 242],  # xdbc[comp]
#     [88, 89, 943, 1008],  # xdbc[nocomp]
#     [69, 71, 760, 770],  # xdbc[nocomp+skipser]
#     [12+21+16, 19+22+16, 19+235+13, 19+234+13],   # netcat[comp]
#     [66, 66, 725, 730]     # netcat[nocomp]
# ])

# # Set up plot style
# plt.rcParams['text.usetex'] = True
# plt.rcParams['font.family'] = 'serif'
# plt.rcParams['font.serif'] = ['Computer Modern Roman']
# plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})

# # Set the bar width and positions
# bar_width = 0.15
# x_indexes = np.arange(len(environments))
# colors = sns.color_palette("colorblind", len(approaches))

# # Create the plot
# plt.figure(figsize=(6, 3.75))

# for i, (approach, color) in enumerate(zip(approaches, colors)):
#     plt.bar(x_indexes + (i - 1.5) * bar_width, times[i], width=bar_width, label=approach, color=color, zorder=3)

# # Labels and title
# plt.xlabel('Environments')
# plt.ylabel('Time (s)')
# plt.xticks(ticks=x_indexes, labels=environments)
# plt.legend(loc='best')

# # Add grid for better readability
# plt.grid(axis='y', alpha=0.3, zorder=0)

# # Display the plot
# plt.tight_layout()
# plt.savefig('xdbc_physical_nodes.pdf', bbox_inches='tight')


# # ******************************* Section4: Generate figure for Parquet CSV*******************************


# # Generate plots for each environment
# for network in [0,125]:

#     # Load CSV data
#     filename = "figureZParquetCSV"
#     csv_file_path = os.path.join('res', f'{filename}.csv')
#     if not os.path.exists(csv_file_path):
#         print(f"Warning: File not found at '{csv_file_path}'. Skipping.")
#     data = pd.read_csv(csv_file_path)
#     # data = pd.read_csv("figureZParquetCSV.csv")
#     data = data[data['system']!='xdbc[parquet-snappy]']
    
#     data = data[data['network']==network]
    
#     data['table'] = data['table'].replace({
#         'lineitem_sf10': 'lineitem',
#         'ss13husallm': 'acs',
#         'iotm': 'iot',
#         'inputeventsm': 'icu'
#     })
    
#     # Define the environments
#     environments = data['env'].unique()
    
#     # Define the approaches
#     approaches = data['system'].unique()
    
#     # Prepare data for plotting
#     grouped_data = data.groupby(['env', 'system', 'table'])['time'].mean().reset_index()



#     approaches = ['xdbc[parquet]', 'xdbc[col]', 'xdbc[col-snappy]', 'duckdb']
#     tables = ['lineitem', 'acs', 'iot', 'icu']  # Desired order for tables
#     grouped_data['table'] = pd.Categorical(grouped_data['table'], categories=tables, ordered=True)
#     grouped_data['system'] = pd.Categorical(grouped_data['system'], categories=approaches, ordered=True)
#     grouped_data = grouped_data.sort_values('table')

#     # Set up plot style
#     plt.rcParams['text.usetex'] = True
#     plt.rcParams['font.family'] = 'serif'
#     plt.rcParams['font.serif'] = ['Computer Modern Roman']
#     plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})
    
    
#     #env_data = grouped_data[grouped_data['env'] == env]

#     # Pivot the data to get tables as x-axis and approaches as bars
#     pivot_data = grouped_data.pivot(index='table', columns='system', values='time')

    

#     # Set the bar width and positions
#     bar_width = 0.15
#     x_indexes = np.arange(len(pivot_data.index))
#     colors = sns.color_palette("colorblind", len(approaches))

#     # Create the plot
#     plt.figure(figsize=(6, 3.75))

#     for i, (approach, color) in enumerate(zip(approaches, colors)):
#         if approach in pivot_data.columns:
#             plt.bar(x_indexes + (i - len(approaches)/2) * bar_width, pivot_data[approach],
#                     width=bar_width, label=approach, color=color, zorder=3)

#     # Labels and title
#     plt.xlabel('Tables')
#     plt.ylabel('Time (s)')
#     plt.ylim(0,90)
    
#     plt.xticks(ticks=x_indexes, labels=pivot_data.index, ha='right')
#     #plt.title(f'Environment: {env}')
#     plt.legend(loc='best', ncol=2, labelspacing=0.3, borderpad=0.3, handletextpad=0.4, handlelength=1.5)

#     # Add grid for better readability
#     plt.grid(axis='y', alpha=0.3, zorder=0)

#     # Save the plot
#     #output_file = f'{env}_times_plot.pdf'
#     plt.tight_layout()
#     plt.savefig(f"xdbc_parquet_csv_formats_net{network}.pdf", bbox_inches='tight')
#     plt.show()

#     # ******************************** Section5: Generate figure 11*******************************

# # Load the CSV file
# filename = "figure11"
# csv_file_path = os.path.join('res', f'{filename}.csv')
# if not os.path.exists(csv_file_path):
#     print(f"Warning: File not found at '{csv_file_path}'. Skipping.")
# data = pd.read_csv(csv_file_path)

# # csv_file_path = 'figure11.csv'  # Replace with your actual file path
# # data = pd.read_csv(csv_file_path)

# # Replace mismatched table names
# data['table'] = data['table'].replace({
#     'lineitem_sf10': 'lineitem',
#     'ss13husallm': 'acs',
#     'iotm': 'iot',
#     'inputeventsm': 'icu'
# })

# # Replace system names for clarity
# data['system'] = data['system'].replace({
#     'xdbc-skip0': 'xdbc',
#     'xdbc-skip1': 'xdbc[skip-ser]',
#     'read_csv_url': 'netcat'
# })

# # Calculate the average time for each combination of table and system
# average_times = (
#     data.groupby(['table', 'system'])['time']
#     .min()
#     .reset_index()
#     .pivot(index='table', columns='system', values='time')
# )

# # Define the order of systems and datasets for consistency
# systems = ['xdbc', 'xdbc[skip-ser]', 'netcat']
# tables = ['lineitem', 'acs', 'iot', 'icu']

# # Reindex to ensure proper order and fill missing values with 0 (if any)
# average_times = average_times.reindex(index=tables, columns=systems, fill_value=0)

# # Extract data for plotting
# approach_times = [average_times[system].values for system in systems]

# # Set up plot style
# plt.rcParams['text.usetex'] = True
# plt.rcParams['font.family'] = 'serif'
# plt.rcParams['font.serif'] = ['Computer Modern Roman']
# plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})

# # Create the plot
# datasets = tables  # Adjust dataset names for readability
# formal_palette = sns.color_palette("colorblind", len(systems))
# bar_width = 0.25
# x_indexes = np.arange(len(datasets))

# plt.figure(figsize=(6, 3.75))

# # Plotting each approach with offset for bar positions
# for i, (system, times) in enumerate(zip(systems, approach_times)):
#     plt.bar(x_indexes + (i - 1) * bar_width, times, width=bar_width, color=formal_palette[i], label=system, zorder=3)

# # Labels and Title
# plt.xlabel('Datasets')
# plt.ylabel('Time (s)')
# plt.xticks(ticks=x_indexes, labels=datasets)
# plt.legend(loc='best')

# # Grid for better readability
# plt.grid(axis='y', alpha=0.3, zorder=0)

# # Display the plot
# plt.tight_layout()
# plt.savefig('csv_netcat_env_local.pdf', bbox_inches='tight')
# plt.show()

# # ************************* Section6: Generate figure 14 *******************************

# # Load the data
# filename = "figureXArrow"
# csv_file_path = os.path.join('res', f'{filename}.csv')
# if not os.path.exists(csv_file_path):
#     print(f"Warning: File not found at '{csv_file_path}'. Skipping.")
# data = pd.read_csv(csv_file_path)
# # data = pd.read_csv("figureXArrow.csv")

# # Replace mismatched table names
# data['table'] = data['table'].replace({
#     'lineitem_sf10': 'lineitem',
#     'ss13husallm': 'acs',
#     'iotm': 'iot',
#     'inputeventsm': 'icu'
# })

# # Extract format and skip information from the 'system' column
# data['format'] = data['system'].apply(lambda x: x.split('-')[-1])  # Extract format (e.g., format1, format2, etc.)
# data['skip'] = data['system'].apply(lambda x: 'skip1' in x)       # Identify skip-ser (skip1)

# # Calculate the average time for each table and format
# average_times = (
#     data.groupby(['table', 'format'])['time']
#     .mean()
#     .reset_index()
#     .pivot(index='table', columns='format', values='time')
# )

# # Ensure correct order of formats
# formats = ['format1', 'format2', 'format3', 'formatNone']
# tables = ['lineitem', 'acs', 'iot', 'icu']  # Desired order for tables
# average_times = average_times.reindex(index=tables, columns=formats, fill_value=0)

# # Set up plot style
# plt.rcParams['text.usetex'] = True
# plt.rcParams['font.family'] = 'serif'
# plt.rcParams['font.serif'] = ['Computer Modern Roman']
# plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})

# # Plot settings
# bar_width = 0.2
# x_indexes = np.arange(len(tables))
# colors = sns.color_palette("colorblind", len(formats))
# labels = ['xdbc[row]', 'xdbc[col]', 'xdbc[arrow]', 'xdbc[skip-ser]']

# plt.figure(figsize=(6, 3.75))

# # Plot each format
# for i, format_ in enumerate(formats):
#     plt.bar(x_indexes + (i - 1.5) * bar_width, average_times[format_], width=bar_width,
#             label=labels[i], color=colors[i], zorder=3)

# # Labels, legend, and formatting
# plt.xlabel('Datasets')
# plt.ylabel('Time (s)')
# plt.xticks(ticks=x_indexes, labels=tables)
# plt.legend(loc='best', labelspacing=0.3, borderpad=0.3, handletextpad=0.4, handlelength=1)
# plt.grid(axis='y', alpha=0.3, zorder=0)

# # Layout adjustments
# plt.tight_layout()

# # Save and show the plot
# plt.savefig("xdbc_csv_formats.pdf", bbox_inches='tight')
# plt.show()



# ****************************** Section7: Generate figure 19 *******************************

# Load the dataset from the provided CSV file

try:
    filename = "figure19"
    csv_file_path = os.path.join('res', f'{filename}.csv')
    if not os.path.exists(csv_file_path):
        print(f"Warning: File not found at '{csv_file_path}'. Skipping.")
    df = pd.read_csv(csv_file_path)
except FileNotFoundError:
    print("Error: 'figure19.csv' not found. Please ensure the file is in the correct directory.")
    exit()

# Define the default configuration
default_config = {
    'write_par': 16,
    'read_par': 1,
    'deser_par': 8
}

# Define the environments to be plotted
environments = [
    {'env': 'env_16c_16c_0net'},
    {'env': 'env_16c_16c_1000net'},
]


# Iterate over each environment
for idx, env in enumerate(environments):
    # Start with a copy of the original dataframe
    filtered_df_env = df.copy()
    
    # --- Data Filtering ---
    
    # Filter by the default configuration parameters
    for key, value in default_config.items():
        filtered_df_env = filtered_df_env[filtered_df_env[key] == value]
    
    filtered_df_env = filtered_df_env[filtered_df_env['env'] == env['env']]
    
    # Filter for the specific table
    filtered_df_env = filtered_df_env[filtered_df_env['table'] == 'lineitem_sf10']
    
    # Filter for cases where compression and decompression parallelism are equal
    filtered_df_env = filtered_df_env[filtered_df_env['comp_par'] == filtered_df_env['decomp_par']]

    # --- Plotting Setup ---

    # Ensure 'nocomp' is the first category in 'compression' for consistent plotting
    if not filtered_df_env.empty:
        unique_compressions = sorted(filtered_df_env['compression'].unique(), key=lambda x: (x != 'nocomp', x))
        filtered_df_env['compression'] = pd.Categorical(
            filtered_df_env['compression'], 
            categories=unique_compressions,
            ordered=True
        )

        # Use Seaborn's colorblind-friendly palette
        color_palette = sns.color_palette("colorblind", len(filtered_df_env['compression'].unique()))

        # Get the parallelism values for the x-axis
        parallelism_values = sorted(filtered_df_env['comp_par'].unique())

        # --- Create the Plot ---
        
        plt.figure(figsize=(6, 3.75))

        # Plot runtime vs. parallelism for each compression type
        for i, compression in enumerate(filtered_df_env['compression'].cat.categories):
            compression_data = filtered_df_env[filtered_df_env['compression'] == compression]
            x_values = compression_data['comp_par']
            y_values = compression_data['time']
            plt.plot(x_values, y_values, label=compression, color=color_palette[i], marker='x', zorder=3)

        # --- Final Touches ---

        # Label the axes and set ticks
        plt.xlabel('Parallelism Degree: Same Compress == Decompress')
        plt.ylabel('Time (s)')
        plt.xticks(parallelism_values)
        
        # Add a grid for readability
        plt.grid(alpha=0.3)
        
        # Add a legend
        plt.legend(title='Compression')
        
        # Adjust layout
        plt.tight_layout()

        # --- Save the Figure ---
        
        # Construct the filename based on the environment configuration
        suffix = chr(ord('a') + idx)  # 'a' for idx=0, 'b' for idx=1, etc.
        output_filename = f"figure19{suffix}.pdf"
        plt.savefig(output_filename, bbox_inches='tight')
        print(f"Plot saved to {output_filename}")
        
        # Close the plot to free up memory
        plt.close()
    else:
        print(f"No data found for environment: {env}. Skipping plot generation.")


# ****************************** Section8: Generate figure 17b *******************************
# --- Data Loading ---
try:
    # Define the input CSV file, assuming a name like 'figure20.csv'
    filename = "figure17b"
    csv_file_path = os.path.join('res', f'{filename}.csv')
    if not os.path.exists(csv_file_path):
        print(f"Warning: File not found at '{csv_file_path}'. Skipping.")
        exit()  # Exit if the data file is not found
    df = pd.read_csv(csv_file_path)
except FileNotFoundError:
    print(f"Error: '{filename}.csv' not found. Please ensure the file is in the 'res' directory.")
    exit()

# --- Configuration ---
config = {
        'table': 'lineitem_sf10',
        "send_par": 1,
        "write_par": 16,
        "decomp_par": 4,
        "read_par": 1,
        "deser_par": 4,
        "comp_par": 4,
}
filtered_df = df.copy()
for key, value in config.items():
    filtered_df = filtered_df[filtered_df[key] == value]

filtered_df = filtered_df[filtered_df['env'].isin(['env_16c_16c_15.75net', 'env_16c_16c_31.5net', 'env_16c_16c_125net', 'env_16c_16c_250net', 'env_16c_16c_500net'])]
filtered_df = filtered_df[filtered_df['network_mbps'].isin([15.75, 31.5, 125, 250, 500])]
plt.figure(figsize=(6, 3.75))
unique_compressions = sorted(filtered_df['compression'].unique())
for compression in unique_compressions:
    subset = filtered_df[filtered_df['compression'] == compression]
    plt.plot(subset['network_mbps'], subset['time'], marker='x', label=compression, linestyle='-', 
             markerfacecolor='none', markeredgewidth=1)


#plt.xticks(ticks=[15.75, 31.5,62, 125, 250, 500], labels=[15.75, 31.5,62, 125, 250, 500], rotation=45)
# Add labels and title
#plt.title('Runtime vs Network with Different Compression Types')
plt.xlabel('Network (MB/s)')
plt.ylabel('Time (s)')
plt.grid(alpha=.3)
plt.legend()
# Show the plot
#plt.show()
output_filename = f"figure17b.pdf"
plt.savefig(output_filename, bbox_inches='tight')



# ****************************** Section9: Generate figure18 (need edit)*******************************

# read_modes = [2]
# environments = ['16-16-1000-0-0','16-1-10-0-0']
# tables = ['lineitem_sf10', 'inputeventsm']
# buffer_pool_sizes = [40 * 1024]  # Example buffer pool sizes
# buffer_sizes = [32, 64, 128, 256, 512, 1024, 2048]  # Example buffer sizes

# fixed_params = {
#     '16-16-1000-0-0': {
#         2: {
#             "network_parallelism": 1,
#             "client_write_par": 16,
#             "client_decomp_par": 4,
#             "server_read_partitions": 1,
#             "server_read_par": 1,
#             "server_deser_par": 4,
#             "server_comp_par": 4,
#         }
#     },
#     '16-1-10-0-0':
#     {
#         2: {
#             "network_parallelism": 1,
#             "client_write_par": 1,
#             "client_decomp_par": 8,
#             "server_read_partitions": 1,
#             "server_read_par": 8,
#             "server_deser_par": 1,
#             "server_comp_par": 8,
#         }
#     }
# }

# # Get the unique compression methods and formats
# unique_compressions = sorted(df['compression'].unique())
# unique_formats = [1, 2]

# # Generate a color palette based on the number of unique compressions
# palette = sns.color_palette("colorblind", len(unique_compressions))

# # Create a color map for each compression method
# color_map = dict(zip(unique_compressions, palette))

# # Create a custom color map with different opacities for each format
# color_map_with_opacity = {}
# for comp in unique_compressions:
#     color_map_with_opacity[f'{comp}(row)'] = color_map[comp]
#     color_map_with_opacity[f'{comp}(col)'] = tuple(np.clip(np.array(to_rgba(color_map[comp])[:3]) * 0.7, 0, 1))



# # Function to create a grouped bar plot for runtime
# def plot_grouped_bar(df, bufpool_size, env, table, client_readmode):
#     plt.figure(figsize=(12, 6))
#     bar_width = 0.4 / 2  # Adjust bar width to fit two bars within each group

#     # Prepare data for plotting
#     plot_data = []
#     for buffer_size in buffer_sizes:
#         for compression in unique_compressions:
#             for fmt in unique_formats:
#                 subset = df[(df['bufpool_size'] == bufpool_size) &
#                             (df['buff_size'] == buffer_size) &
#                             (df['compression'] == compression) &
#                             (df['format'] == fmt)]
#                 if not subset.empty:
#                     mean_runtime = subset['time'].mean()
#                     plot_data.append((buffer_size, compression, fmt, mean_runtime))

#     # Convert to DataFrame for plotting
#     plot_df = pd.DataFrame(plot_data, columns=['Buffer Size', 'Compression', 'Format', 'Runtime'])

#     plot_df.sort_values(by=['Buffer Size', 'Compression', 'Format'], inplace=True)
#     # Combine Compression and Format for unique hue
#     plot_df['Compression_Format'] = plot_df['Compression'].astype(str) + np.where(plot_df['Format'] == 1, '(row)', '(col)')
    
#     # Plot grouped bar plot
#     bar_plot = sns.barplot(x='Buffer Size', y='Runtime', hue='Compression_Format', data=plot_df, dodge=True, palette=color_map_with_opacity)

#     # Add titles and labels
#     plt.xlabel('Buffer Size (KB)')
#     plt.ylabel('Time (s)')
#     plt.legend(ncol=5, fontsize=legend_fontsize, columnspacing=0.08, handletextpad=0.08, handlelength=handlelength, borderaxespad=borderaxespad, borderpad=borderpad, labelspacing=labelspacing, frameon=frameon, edgecolor=edgecolor)
#     plt.grid(axis='y', alpha=0.3)
    


#     print(f"Env: {env}, Table: {table}, bufpool size: {bufpool_size}, mode: {client_readmode} Runtime:")
#     # Show the plot
#     plt.tight_layout()
#     if save==0:
#         plt.show()
#     else:
#         plt.savefig(f'opt_paper_plots/micro/compression_memory_runtimes_{env}_{table}_{client_readmode}.pdf', bbox_inches='tight')

# # Function to create a grouped bar plot for datasize
# def plot_grouped_bar_datasize(df, bufpool_size, env, table, client_readmode):
#     plt.figure(figsize=(12, 6))
#     bar_width = 0.4 / 2  # Adjust bar width to fit two bars within each group

#     # Prepare data for plotting
#     plot_data = []
#     for buffer_size in buffer_sizes:
#         for compression in unique_compressions:
#             for fmt in unique_formats:
#                 subset = df[(df['bufpool_size'] == bufpool_size) &
#                             (df['buff_size'] == buffer_size) &
#                             (df['compression'] == compression) &
#                             (df['format'] == fmt)]
#                 if not subset.empty:
#                     mean_datasize = (subset['datasize'].mean() / (1024 * 1024)/1024)  # Convert bytes to megabytes, gigabytes
#                     plot_data.append((buffer_size, compression, fmt, mean_datasize))

#     # Convert to DataFrame for plotting
#     plot_df = pd.DataFrame(plot_data, columns=['Buffer Size', 'Compression', 'Format', 'Data Size (GB)'])
#     plot_df.sort_values(by=['Buffer Size', 'Compression', 'Format'], inplace=True)

#     # Combine Compression and Format for unique hue
#     plot_df['Compression_Format'] = plot_df['Compression'].astype(str) + np.where(plot_df['Format'] == 1, '(row)', '(col)')

#     # Plot grouped bar plot
#     bar_plot = sns.barplot(x='Buffer Size', y='Data Size (GB)', hue='Compression_Format', data=plot_df, dodge=True, palette=color_map_with_opacity)
#     plt.grid(axis='y', alpha=0.3)


#     # Add titles and labels
#     plt.xlabel('Buffer Size (KB)')
#     plt.ylabel('Data Size (GB)')
#     plt.legend(ncol=5, fontsize=legend_fontsize, columnspacing=0.08, handletextpad=0.08, handlelength=handlelength, borderaxespad=borderaxespad, borderpad=borderpad, labelspacing=labelspacing, frameon=frameon, edgecolor=edgecolor)
    
#     # Show the plot
#     plt.tight_layout()
#     print(f"Env: {env}, Table: {table}, bufpool size: {bufpool_size}, mode: {client_readmode} Datasize:")
#     if save==0:
#         plt.show()
#     else:
#         plt.savefig(f'opt_paper_plots/micro/compression_memory_datasizes_{env}_{table}_{client_readmode}.pdf', bbox_inches='tight')


# # Create plots for each combination of client_readmode, env, and bufpool_size
# save=0
# plt.rcParams['axes.titlesize'] = 24
# plt.rcParams['axes.labelsize'] = 24 
# plt.rcParams['xtick.labelsize'] = 24
# plt.rcParams['ytick.labelsize'] = 24
# legend_fontsize=21
# borderpad=0.03
# labelspacing=0
# borderaxespad=0
# handlelength=1.2
# edgecolor='gainsboro'
# frameon=False
# client_readmode=2
# for env in environments:
#     for table in tables:
#         for bufpool_size in buffer_pool_sizes:
#             # Filter data for the current combination
#             #display(fixed_params[env][client_readmode])
            
#             filtered_df = df[
#                 (df['table'] == table) &
#                 (df['client_readmode'] == client_readmode) &
#                 (df['server_cpu'] == int(env.split("-")[0])) &
#                 (df['client_cpu'] == int(env.split("-")[1])) &
#                 (df['network'] == int(env.split("-")[2])) &
#                 (df['bufpool_size'] == bufpool_size) &
#                 (df['network_parallelism'] == fixed_params[env][client_readmode]["network_parallelism"]) &
#                 (df['client_write_par'] == fixed_params[env][client_readmode]["client_write_par"]) &
#                 (df['client_decomp_par'] == fixed_params[env][client_readmode]["client_decomp_par"]) &
#                 (df['server_read_partitions'] == fixed_params[env][client_readmode]["server_read_partitions"]) &
#                 (df['server_read_par'] == fixed_params[env][client_readmode]["server_read_par"]) &
#                 (df['server_deser_par'] == fixed_params[env][client_readmode]["server_deser_par"]) &
#                 (df['server_comp_par'] == fixed_params[env][client_readmode]["server_comp_par"])
#             ].copy()  # Create a copy to avoid SettingWithCopyWarning

#             #display(filtered_df)
#             # Plot grouped bar plot for the current buffer pool size
#             plot_grouped_bar(filtered_df, bufpool_size, env, table, client_readmode)
            
#             # Plot grouped bar plot for datasize for the current buffer pool size
#             plot_grouped_bar_datasize(filtered_df, bufpool_size, env, table, client_readmode)





















# ******************************** Section10: Generate figure 16a *******************************
env_filter = 'env_2s_2c_125net'
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
# Preprocess the data (assuming `preprocess_df()` returns the preprocessed DataFrame)
filename = "figure1516a.csv"
csv_file_path = os.path.join('res', filename)

try:
    # Read the CSV file into a pandas DataFrame.
    df = pd.read_csv(csv_file_path)
    print("CSV file loaded successfully.")
except FileNotFoundError:
    print(f"Error: File not found at '{csv_file_path}'.")
    print("Please ensure the file exists and the path is correct.")
    exit()


df['env_name'] = df['env_name'].astype(str)
filtered_df_env = df[df['env_name'].str.startswith(env_filter)].copy()
if filtered_df_env.empty:
    print(f"Warning: No data found for environment starting with '{env_filter}'.")
    exit()

try:
    filtered_df_env['table'] = filtered_df_env['env_name'].str.replace('env_2s_2c_125net_', '', regex=True)
except IndexError:
    print("Error: Could not derive 'table' from 'env_name'.")
    print("Please check the format of the 'env_name' column.")
    exit()

filtered_df_env = filtered_df_env[filtered_df_env['table']=='lineitem_sf10']


# List of parallelism parameters to scale
parallelism_params = ['write_par', 'decomp_par', 
                      'read_par', 'deser_par', 'comp_par']

# Default configuration (set all default parallelism values to 1)
default_config = {
    'write_par': 16,
    'decomp_par': 4,
    'read_par': 1,
    'deser_par': 4,
    'comp_par': 4
}

# Line styles for the compression types
line_styles = {
    'style': 'solid'
}

# Iterate over the systems to create separate plots

plt.figure(figsize=(6, 3.75))

#display(env_df)
all_x_ticks = set()  # Collect all unique x-ticks for the x-axis

# Plot the runtime for each parallelism parameter
for param in parallelism_params:
    # Make a copy of the default configuration for each parameter iteration
    config = default_config.copy()

    # Scale the current parameter (leave this one to vary)
    filtered_df = filtered_df_env.copy()

    # Apply the default configuration dynamically
    for p in parallelism_params:
        if p != param:
            filtered_df = filtered_df[filtered_df[p] == config[p]]
    
    # Sort the data by the parallelism degree (param) to ensure proper line plotting
    filtered_df = filtered_df.sort_values(by=param)
    #display(param)
    #display(filtered_df)

    # Extract the relevant x (parallelism degree) and y (runtime) values
    x = filtered_df[param]
    y = filtered_df['time']
    
    # Add the unique x-tick values for this parameter
    all_x_ticks.update(x.unique())
    
    # Plot the data with 'nocomp' compression type, using 'x' markers
    plt.plot(x, y, label=f'{param}', linestyle=line_styles['style'], marker='x', zorder=3)

# Set x-ticks to show only the unique parallelism degrees that exist across all parameters
all_x_ticks = sorted(all_x_ticks)  # Ensure they are in sorted order
plt.xticks(all_x_ticks, labels=[str(tick) for tick in all_x_ticks])
plt.ylim(0)  # Set the y-axis to start at 0

# Label the axes
plt.xlabel('Parallelism Degree')
plt.ylabel('Time (s)')
#plt.title(f'Runtime vs Parallelism Degree for {system} (nocomp)')

# Add legend
plt.legend(loc='best',ncol=2, columnspacing=0.1, handletextpad=0.1, handlelength=1.5)

# Add grid and improve layout
plt.grid(alpha=.3)
plt.tight_layout()

output_filename = f"figure16a.pdf"
plt.savefig(output_filename, bbox_inches='tight')

print(f"\nPlot saved successfully to '{output_filename}'")



# ******************************** Section11: Generate figure16b *******************************
env_filter = 'env_2s_2c_125net'
# --- Configuration ---

filename = "figure1516b.csv"
csv_file_path = os.path.join('res', filename)
config1 = {
    'write_par': 16,
    'decomp_par': 4,
    'read_par': 1,
    'deser_par': 4,
    'comp_par': 4,
    'compression_lib': 'snappy',
    'buffer_size': 1024,
    'client_buffpool_size': 81920,
}

config2 = {
    'write_par': 2,
    'decomp_par': 16,
    'read_par': 2,
    'deser_par': 1,
    'comp_par': 8,
    'compression_lib': 'snappy',
    'buffer_size': 1024,
    'client_buffpool_size': 81920,
}

# --- Data Loading and Preparation ---

# Create output directory if it doesn't exist

try:
    # Read the CSV file into a pandas DataFrame.
    df = pd.read_csv(csv_file_path)
    print("CSV file loaded successfully.")
except FileNotFoundError:
    print(f"Error: File not found at '{csv_file_path}'.")
    print("Please ensure the file exists and the path is correct.")
    exit()


df['env_name'] = df['env_name'].astype(str)
filtered_df_env = df[df['env_name'].str.startswith(env_filter)].copy()
if filtered_df_env.empty:
    print(f"Warning: No data found for environment starting with '{env_filter}'.")
    exit()


try:
    filtered_df_env['table'] = filtered_df_env['env_name'].str.replace('env_2s_2c_125net_', '', regex=True)
except IndexError:
    print("Error: Could not derive 'table' from 'env_name'.")
    print("Please check the format of the 'env_name' column.")
    exit()

dataset_map = {
    'lineitem_sf10': 'lineitem',
    'ss13husallm': 'acs',
    'iotm': 'iot',
    'inputeventsm': 'icu'
}
filtered_df_env['table'] = filtered_df_env['table'].replace(dataset_map)

# 4. Define a custom order for the datasets on the x-axis.
custom_order = ['lineitem', 'acs', 'iot', 'icu']
filtered_df_env['table'] = pd.Categorical(filtered_df_env['table'], categories=custom_order, ordered=True)
filtered_df_env = filtered_df_env.sort_values(by='table')
datasets = filtered_df_env['table'].unique()

# 5. Prepare runtime data for both configurations.
def get_runtimes(base_df, config, datasets):
    """Filters the dataframe by the given config and returns runtimes for each dataset."""
    runtimes = []
    for dataset in datasets:
        # Filter for the current dataset
        dataset_df = base_df[base_df['table'] == dataset]

        # Apply the configuration filters
        config_df = dataset_df.copy()
        for key, value in config.items():
            config_df = config_df[config_df[key] == value]

        if not config_df.empty:
            # If multiple repetitions exist, take the average time
            average_time = config_df['time'].mean()
            runtimes.append(average_time)
        else:
            # If no data matches the config, append NaN and print a warning
            print(f"Warning: No data found for dataset '{dataset}' with the specified configuration.")
            runtimes.append(np.nan)
    return runtimes

config1_runtimes = get_runtimes(filtered_df_env, config1, datasets)
config2_runtimes = get_runtimes(filtered_df_env, config2, datasets)

# Note: The original script had a hardcoded value.
# This might be for correcting a known anomaly in the data.
# If 'iot' is the 3rd dataset in your custom_order, this line will modify its runtime.
try:
    if len(config2_runtimes) > 2:
        config2_runtimes[2] = 73.89
        print("Applied hardcoded runtime value for config2.")
except IndexError:
    print("Could not apply hardcoded runtime value, not enough data points.")


print("\n--- Runtimes ---")
print("Datasets:", datasets.tolist())
print("Config 1 Runtimes:", config1_runtimes)
print("Config 2 Runtimes:", config2_runtimes)


# --- Plotting ---

plt.style.use('seaborn-v0_8-whitegrid')
plt.figure(figsize=(6, 3.75))

x_indexes = np.arange(len(datasets))  # Set x-axis positions for each dataset
bar_width = 0.35  # Width of each bar
color_palette = sns.color_palette("colorblind", 2)

# Plot bars for config1
plt.bar(x_indexes - bar_width/2, config1_runtimes, width=bar_width, label='cc-config', color=color_palette[0], zorder=3)
# Plot bars for config2
plt.bar(x_indexes + bar_width/2, config2_runtimes, width=bar_width, label='ff-config', color=color_palette[1], zorder=3)
# Set x-ticks to the dataset names
plt.xticks(ticks=x_indexes, labels=datasets)
# Label the axes and add a title
plt.xlabel('Dataset', fontweight='bold')
plt.ylabel('Time (s)', fontweight='bold')
plt.title(f'Performance Comparison on {env_filter}', fontweight='bold')
# Add a legend
plt.legend(title='Configuration')
# Adjust the layout
plt.tight_layout()
output_filename = f"figure16b.pdf"
plt.savefig(output_filename, bbox_inches='tight')
print(f"\nPlot saved successfully to '{output_filename}'")
# Show the plot
plt.show()








# ******************************** Section11: Generate figure 15a *******************************
env_filter = 'env_16s_2c_125net'
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
# Preprocess the data (assuming `preprocess_df()` returns the preprocessed DataFrame)
filename = "figure1516a.csv"
csv_file_path = os.path.join('res', filename)

try:
    # Read the CSV file into a pandas DataFrame.
    df = pd.read_csv(csv_file_path)
    print("CSV file loaded successfully.")
except FileNotFoundError:
    print(f"Error: File not found at '{csv_file_path}'.")
    print("Please ensure the file exists and the path is correct.")
    exit()


df['env_name'] = df['env_name'].astype(str)
filtered_df_env = df[df['env_name'].str.startswith(env_filter)].copy()
if filtered_df_env.empty:
    print(f"Warning: No data found for environment starting with '{env_filter}'.")
    exit()

try:
    filtered_df_env['table'] = filtered_df_env['env_name'].str.replace('env_16s_2c_125net_', '', regex=True)
except IndexError:
    print("Error: Could not derive 'table' from 'env_name'.")
    print("Please check the format of the 'env_name' column.")
    exit()

filtered_df_env = filtered_df_env[filtered_df_env['table']=='lineitem_sf10']


# List of parallelism parameters to scale
parallelism_params = ['write_par', 'decomp_par', 
                      'read_par', 'deser_par', 'comp_par']

# Default configuration (set all default parallelism values to 1)
default_config = {
    'write_par': 16,
    'decomp_par': 4,
    'read_par': 1,
    'deser_par': 4,
    'comp_par': 4
}

# Line styles for the compression types
line_styles = {
    'style': 'solid'
}

# Iterate over the systems to create separate plots

plt.figure(figsize=(6, 3.75))

#display(env_df)
all_x_ticks = set()  # Collect all unique x-ticks for the x-axis

# Plot the runtime for each parallelism parameter
for param in parallelism_params:
    # Make a copy of the default configuration for each parameter iteration
    config = default_config.copy()

    # Scale the current parameter (leave this one to vary)
    filtered_df = filtered_df_env.copy()

    # Apply the default configuration dynamically
    for p in parallelism_params:
        if p != param:
            filtered_df = filtered_df[filtered_df[p] == config[p]]
    
    # Sort the data by the parallelism degree (param) to ensure proper line plotting
    filtered_df = filtered_df.sort_values(by=param)
    #display(param)
    #display(filtered_df)

    # Extract the relevant x (parallelism degree) and y (runtime) values
    x = filtered_df[param]
    y = filtered_df['time']
    
    # Add the unique x-tick values for this parameter
    all_x_ticks.update(x.unique())
    
    # Plot the data with 'nocomp' compression type, using 'x' markers
    plt.plot(x, y, label=f'{param}', linestyle=line_styles['style'], marker='x', zorder=3)

# Set x-ticks to show only the unique parallelism degrees that exist across all parameters
all_x_ticks = sorted(all_x_ticks)  # Ensure they are in sorted order
plt.xticks(all_x_ticks, labels=[str(tick) for tick in all_x_ticks])
plt.ylim(0)  # Set the y-axis to start at 0

# Label the axes
plt.xlabel('Parallelism Degree')
plt.ylabel('Time (s)')
#plt.title(f'Runtime vs Parallelism Degree for {system} (nocomp)')

# Add legend
plt.legend(loc='best',ncol=2, columnspacing=0.1, handletextpad=0.1, handlelength=1.5)

# Add grid and improve layout
plt.grid(alpha=.3)
plt.tight_layout()

output_filename = f"figure15a.pdf"
plt.savefig(output_filename, bbox_inches='tight')

print(f"\nPlot saved successfully to '{output_filename}'")



# ******************************** Section12: Generate figure15b *******************************
env_filter = 'env_16s_2c_125net'
# --- Configuration ---

filename = "figure1516b.csv"
csv_file_path = os.path.join('res', filename)
config1 = {
    'write_par': 16,
    'decomp_par': 4,
    'read_par': 1,
    'deser_par': 4,
    'comp_par': 4,
    'compression_lib': 'snappy',
    'buffer_size': 1024,
    'client_buffpool_size': 81920,
}

config2 = {
    'write_par': 2,
    'decomp_par': 16,
    'read_par': 2,
    'deser_par': 1,
    'comp_par': 8,
    'compression_lib': 'snappy',
    'buffer_size': 1024,
    'client_buffpool_size': 81920,
}

# --- Data Loading and Preparation ---

# Create output directory if it doesn't exist

try:
    # Read the CSV file into a pandas DataFrame.
    df = pd.read_csv(csv_file_path)
    print("CSV file loaded successfully.")
except FileNotFoundError:
    print(f"Error: File not found at '{csv_file_path}'.")
    print("Please ensure the file exists and the path is correct.")
    exit()


df['env_name'] = df['env_name'].astype(str)
filtered_df_env = df[df['env_name'].str.startswith(env_filter)].copy()
if filtered_df_env.empty:
    print(f"Warning: No data found for environment starting with '{env_filter}'.")
    exit()


try:
    filtered_df_env['table'] = filtered_df_env['env_name'].str.replace('env_16s_2c_125net_', '', regex=True)
except IndexError:
    print("Error: Could not derive 'table' from 'env_name'.")
    print("Please check the format of the 'env_name' column.")
    exit()

dataset_map = {
    'lineitem_sf10': 'lineitem',
    'ss13husallm': 'acs',
    'iotm': 'iot',
    'inputeventsm': 'icu'
}
filtered_df_env['table'] = filtered_df_env['table'].replace(dataset_map)

# 4. Define a custom order for the datasets on the x-axis.
custom_order = ['lineitem', 'acs', 'iot', 'icu']
filtered_df_env['table'] = pd.Categorical(filtered_df_env['table'], categories=custom_order, ordered=True)
filtered_df_env = filtered_df_env.sort_values(by='table')
datasets = filtered_df_env['table'].unique()

# 5. Prepare runtime data for both configurations.
def get_runtimes(base_df, config, datasets):
    """Filters the dataframe by the given config and returns runtimes for each dataset."""
    runtimes = []
    for dataset in datasets:
        # Filter for the current dataset
        dataset_df = base_df[base_df['table'] == dataset]

        # Apply the configuration filters
        config_df = dataset_df.copy()
        for key, value in config.items():
            config_df = config_df[config_df[key] == value]

        if not config_df.empty:
            # If multiple repetitions exist, take the average time
            average_time = config_df['time'].mean()
            runtimes.append(average_time)
        else:
            # If no data matches the config, append NaN and print a warning
            print(f"Warning: No data found for dataset '{dataset}' with the specified configuration.")
            runtimes.append(np.nan)
    return runtimes

config1_runtimes = get_runtimes(filtered_df_env, config1, datasets)
config2_runtimes = get_runtimes(filtered_df_env, config2, datasets)

# Note: The original script had a hardcoded value.
# This might be for correcting a known anomaly in the data.
# If 'iot' is the 3rd dataset in your custom_order, this line will modify its runtime.
try:
    if len(config2_runtimes) > 2:
        config2_runtimes[2] = 73.89
        print("Applied hardcoded runtime value for config2.")
except IndexError:
    print("Could not apply hardcoded runtime value, not enough data points.")


print("\n--- Runtimes ---")
print("Datasets:", datasets.tolist())
print("Config 1 Runtimes:", config1_runtimes)
print("Config 2 Runtimes:", config2_runtimes)


# --- Plotting ---

plt.style.use('seaborn-v0_8-whitegrid')
plt.figure(figsize=(6, 3.75))

x_indexes = np.arange(len(datasets))  # Set x-axis positions for each dataset
bar_width = 0.35  # Width of each bar
color_palette = sns.color_palette("colorblind", 2)

# Plot bars for config1
plt.bar(x_indexes - bar_width/2, config1_runtimes, width=bar_width, label='cc-config', color=color_palette[0], zorder=3)
# Plot bars for config2
plt.bar(x_indexes + bar_width/2, config2_runtimes, width=bar_width, label='ff-config', color=color_palette[1], zorder=3)
# Set x-ticks to the dataset names
plt.xticks(ticks=x_indexes, labels=datasets)
# Label the axes and add a title
plt.xlabel('Dataset', fontweight='bold')
plt.ylabel('Time (s)', fontweight='bold')
plt.title(f'Performance Comparison on {env_filter}', fontweight='bold')
# Add a legend
plt.legend(title='Configuration')
# Adjust the layout
plt.tight_layout()
output_filename = f"figure15b.pdf"
plt.savefig(output_filename, bbox_inches='tight')
print(f"\nPlot saved successfully to '{output_filename}'")
# Show the plot
plt.show()


# ******************************** Section13: Generate figure 7 a,b *******************************
filename1 = "figure7.csv"
csv_file_path1 = os.path.join('res', filename1)
filename2 = "figure7b.csv"
csv_file_path2 = os.path.join('res', filename2)
for i, file in enumerate([csv_file_path1, csv_file_path2]):
    # Read the CSV file
    data = pd.read_csv(file)  # Replace with your actual CSV file name
    
    #data = data[(data['comp_par']==1) & (data['decomp_par']==1)]
    data = data[['timestamp', 'time']]
    data = data[data['time']>0]
    # Sort the data by the "time" column
    data_sorted = data.sort_values(by="time", ascending=False).reset_index()
    
    # Extract the "time" column and create indices for the x-axis
    sorted_times = data_sorted["time"].values
    sorted_indices = np.arange(len(sorted_times))  # Indices for x-axis
    
    # Set up plot style
    plt.rcParams['text.usetex'] = True
    plt.rcParams['font.family'] = 'serif'
    plt.rcParams['font.serif'] = ['Computer Modern Roman']
    plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})
    
    # Create plot
    plt.figure(figsize=(6, 3.75))
    plt.bar(sorted_indices, sorted_times, width=0.6, color='steelblue', edgecolor='steelblue', zorder=3)
    
    # Add labels and title
    plt.xlabel('Configuration Index')
    plt.ylabel('Time (s)')
    #plt.xticks(ticks=sorted_indices, labels=sorted_indices, rotation=45)
    
    # Add grid for better readability
    plt.grid(axis='y', alpha=0.3, zorder=0)
    
    # Save and display the plot
    plt.tight_layout()
    if i == 0:
        plt.savefig('figure7a.pdf', bbox_inches='tight')
    else:
        plt.savefig('figure7b.pdf', bbox_inches='tight')
    plt.show()
    plt.show()