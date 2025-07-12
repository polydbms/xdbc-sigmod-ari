import pandas as pd
import matplotlib.pyplot as plt
import os  # Add this import at the top
import numpy as np
import seaborn as sns

# ****************************Section1 :Generate figures 12 & 13***************************

# filenames = ['figureACSVCSV','figureACSVCSVOpt']
filenames = ['figureACSVCSV','figureACSVCSVOpt','figureBCSVPG','figureBCSVPGOpt']

output_mapping = {
    'figureACSVCSV': 'par_scale_csv_csv_write_1.pdf',
    'figureACSVCSVOpt': 'par_scale_csv_csv_ser_8.pdf',
    'figureBCSVPG': 'par_scale_postgres_csv_write_1.pdf',
    'figureBCSVPGOpt': 'par_scale_postgres_csv_read_8.pdf',
}

# --- Main Loop ---
# Process each file specified in the `filenames` list.
for filename in filenames:
    # Check if the file exists before trying to load it.
    csv_file_path = os.path.join('res', f'{filename}.csv')
    if not os.path.exists(csv_file_path):
        print(f"Warning: File not found at '{csv_file_path}'. Skipping.")
        continue

    # Load the CSV data from the file.
    df = pd.read_csv(csv_file_path)

    # To handle any repeated experiments, group by the configuration
    # and average the time. This is more robust than dropping duplicates.
    config_cols = [
        'env', 'read_par', 'deser_par', 'comp_par', 'send_par',
        'rcv_par', 'decomp_par', 'ser_par', 'write_par', 'table'
    ]
    df = df.groupby(config_cols, as_index=False)['time'].mean()

    # List of parallelism parameters to plot on the x-axis
    parallelism_keys = [
        'read_par', 'deser_par', 'comp_par',
        'decomp_par', 'ser_par', 'write_par'
    ]

    # Create a new figure for each file.
    plt.figure(figsize=(6, 3.75))

    # The values for the x-axis.
    scale_values = [1, 2, 4, 8, 16]

    # Plot each parameter's scaling behavior.
    for param in parallelism_keys:
        # --- DYNAMIC FILTERING LOGIC ---
        # 1. Define the default baseline configuration for each iteration.
        baseline_config = {
            'read_par': 1, 'deser_par': 1, 'comp_par': 1,
            'decomp_par': 1, 'ser_par': 1, 'write_par': 1
        }

        # 2. Check for the "Opt" special case and adjust the baseline.
        # This makes the script adaptable to different experimental setups.
        if "figureACSVCSVOpt" in filename and param != 'ser_par':
            baseline_config['ser_par'] = 8
        if "figureBCSVPGOpt" in filename and param != 'read_par':
            baseline_config['read_par'] = 8

        # 3. Filter the DataFrame using the correct dynamic baseline.
        filtered_df = df.copy()
        for key, value in baseline_config.items():
            if key != param:  # Don't filter the parameter we are currently plotting
                filtered_df = filtered_df[filtered_df[key] == value]
        # --- END OF LOGIC ---

        # Extract the data for the current parameter.
        filtered_df = filtered_df[filtered_df[param].isin(scale_values)].sort_values(by=param)

        # Plot the results only if data was found.
        if not filtered_df.empty:
            grouped = filtered_df.groupby(param)['time'].mean().reset_index()
            plt.plot(grouped[param], grouped['time'], marker='x', label=f"{param}")

    # --- Final Plot Customization ---
    plt.ylim(bottom=0)
    plt.xlabel('Parallelism Degree')
    plt.ylabel('Time (s)')
    plt.xticks(scale_values)
    plt.legend(loc='best', ncol=3)
    plt.grid(alpha=0.3)
    plt.tight_layout()

    # Save the plot to the corresponding PDF file.
    output_filename = output_mapping.get(filename, f'{filename}.pdf')
    plt.savefig(output_filename, bbox_inches='tight')
    print(f"Plot saved to '{output_filename}'")

    plt.show()



# ******************************Section2: Generate figure for MemoryManagement**************************

    # Load the CSV file
filename = "figureMemoryManagement"
csv_file_path = os.path.join('res', f'{filename}.csv')
if not os.path.exists(csv_file_path):
    print(f"Warning: File not found at '{csv_file_path}'. Skipping.")
data = pd.read_csv(csv_file_path)

data = data[data['table']=='lineitem_sf10_nocomp']  # Filter for the specific table
# Prepare plot data
data['bufferpool_size'] = data['bufferpool_size'].astype(int)
data['buffer_size'] = data['buffer_size'].astype(int)

# Sort buffer pool and buffer sizes for consistency
data = data.sort_values(['bufferpool_size', 'buffer_size'])

# Set up plot style
plt.rcParams['text.usetex'] = True
plt.rcParams['font.family'] = 'serif'
plt.rcParams['font.serif'] = ['Computer Modern Roman']
plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})

# Unique buffer pool sizes and buffer sizes
bufferpool_sizes = sorted(data['bufferpool_size'].unique())
buffer_sizes = sorted(data['buffer_size'].unique())
colors = sns.color_palette("colorblind", len(buffer_sizes))

# Plot
plt.figure(figsize=(6, 3.75))
bar_width = 0.14
x_indexes = np.arange(len(bufferpool_sizes))

for i, buffer_size in enumerate(buffer_sizes):
    subset = data[data['buffer_size'] == buffer_size]
    means = [
        subset[subset['bufferpool_size'] == pool_size]['time'].mean() if pool_size in subset['bufferpool_size'].values else 0
        for pool_size in bufferpool_sizes
    ]
    positions = x_indexes + (i - len(buffer_sizes) / 2) * bar_width
    plt.bar(positions, means, width=bar_width, label=f"{buffer_size}", color=colors[i], zorder=3)

# Formatting
plt.xlabel("Buffer Pool Size (MB)")
plt.ylabel("Time (s)")
plt.ylim(0,34)
plt.xticks(ticks=x_indexes, labels=[f"{int(bp/1024)}" for bp in bufferpool_sizes], ha="center")
plt.legend(title="Buffer Size (KB)",loc='best', ncol=7, labelspacing=0.1, borderpad=0.3, handletextpad=0.2, handlelength=1, columnspacing=.3)
plt.grid(axis='y', alpha=0.3, zorder=0)

# Save and show the plot
plt.tight_layout()
plt.savefig("bufferpool_vs_buffersize.pdf", bbox_inches='tight')
plt.show()



# ******************************Section3: Generate figure for physical nodes**************************
# Define the environments and approaches
environments = ['big-big', 'small-big', 'big-laptop', 'small-laptop']
approaches = ['xdbc[comp]', 'xdbc[nocomp]', 'xdbc[nocomp+skipser]', 'netcat[comp]', 'netcat[nocomp]']

# Define the hardcoded times for each approach and environment
times = np.array([
    [29, 24, 212, 242],  # xdbc[comp]
    [88, 89, 943, 1008],  # xdbc[nocomp]
    [69, 71, 760, 770],  # xdbc[nocomp+skipser]
    [12+21+16, 19+22+16, 19+235+13, 19+234+13],   # netcat[comp]
    [66, 66, 725, 730]     # netcat[nocomp]
])

# Set up plot style
plt.rcParams['text.usetex'] = True
plt.rcParams['font.family'] = 'serif'
plt.rcParams['font.serif'] = ['Computer Modern Roman']
plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})

# Set the bar width and positions
bar_width = 0.15
x_indexes = np.arange(len(environments))
colors = sns.color_palette("colorblind", len(approaches))

# Create the plot
plt.figure(figsize=(6, 3.75))

for i, (approach, color) in enumerate(zip(approaches, colors)):
    plt.bar(x_indexes + (i - 1.5) * bar_width, times[i], width=bar_width, label=approach, color=color, zorder=3)

# Labels and title
plt.xlabel('Environments')
plt.ylabel('Time (s)')
plt.xticks(ticks=x_indexes, labels=environments)
plt.legend(loc='best')

# Add grid for better readability
plt.grid(axis='y', alpha=0.3, zorder=0)

# Display the plot
plt.tight_layout()
plt.savefig('xdbc_physical_nodes.pdf', bbox_inches='tight')


# ******************************* Section4: Generate figure for Parquet CSV*******************************


# Generate plots for each environment
for network in [0,125]:

    # Load CSV data
    filename = "figureZParquetCSV"
    csv_file_path = os.path.join('res', f'{filename}.csv')
    if not os.path.exists(csv_file_path):
        print(f"Warning: File not found at '{csv_file_path}'. Skipping.")
    data = pd.read_csv(csv_file_path)
    # data = pd.read_csv("figureZParquetCSV.csv")
    data = data[data['system']!='xdbc[parquet-snappy]']
    
    data = data[data['network']==network]
    
    data['table'] = data['table'].replace({
        'lineitem_sf10': 'lineitem',
        'ss13husallm': 'acs',
        'iotm': 'iot',
        'inputeventsm': 'icu'
    })
    
    # Define the environments
    environments = data['env'].unique()
    
    # Define the approaches
    approaches = data['system'].unique()
    
    # Prepare data for plotting
    grouped_data = data.groupby(['env', 'system', 'table'])['time'].mean().reset_index()



    approaches = ['xdbc[parquet]', 'xdbc[col]', 'xdbc[col-snappy]', 'duckdb']
    tables = ['lineitem', 'acs', 'iot', 'icu']  # Desired order for tables
    grouped_data['table'] = pd.Categorical(grouped_data['table'], categories=tables, ordered=True)
    grouped_data['system'] = pd.Categorical(grouped_data['system'], categories=approaches, ordered=True)
    grouped_data = grouped_data.sort_values('table')

    # Set up plot style
    plt.rcParams['text.usetex'] = True
    plt.rcParams['font.family'] = 'serif'
    plt.rcParams['font.serif'] = ['Computer Modern Roman']
    plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})
    
    
    #env_data = grouped_data[grouped_data['env'] == env]

    # Pivot the data to get tables as x-axis and approaches as bars
    pivot_data = grouped_data.pivot(index='table', columns='system', values='time')

    

    # Set the bar width and positions
    bar_width = 0.15
    x_indexes = np.arange(len(pivot_data.index))
    colors = sns.color_palette("colorblind", len(approaches))

    # Create the plot
    plt.figure(figsize=(6, 3.75))

    for i, (approach, color) in enumerate(zip(approaches, colors)):
        if approach in pivot_data.columns:
            plt.bar(x_indexes + (i - len(approaches)/2) * bar_width, pivot_data[approach],
                    width=bar_width, label=approach, color=color, zorder=3)

    # Labels and title
    plt.xlabel('Tables')
    plt.ylabel('Time (s)')
    plt.ylim(0,90)
    
    plt.xticks(ticks=x_indexes, labels=pivot_data.index, ha='right')
    #plt.title(f'Environment: {env}')
    plt.legend(loc='best', ncol=2, labelspacing=0.3, borderpad=0.3, handletextpad=0.4, handlelength=1.5)

    # Add grid for better readability
    plt.grid(axis='y', alpha=0.3, zorder=0)

    # Save the plot
    #output_file = f'{env}_times_plot.pdf'
    plt.tight_layout()
    plt.savefig(f"xdbc_parquet_csv_formats_net{network}.pdf", bbox_inches='tight')
    plt.show()

    # ******************************** Section5: Generate figure 11*******************************

# Load the CSV file
filename = "figure11"
csv_file_path = os.path.join('res', f'{filename}.csv')
if not os.path.exists(csv_file_path):
    print(f"Warning: File not found at '{csv_file_path}'. Skipping.")
data = pd.read_csv(csv_file_path)

# csv_file_path = 'figure11.csv'  # Replace with your actual file path
# data = pd.read_csv(csv_file_path)

# Replace mismatched table names
data['table'] = data['table'].replace({
    'lineitem_sf10': 'lineitem',
    'ss13husallm': 'acs',
    'iotm': 'iot',
    'inputeventsm': 'icu'
})

# Replace system names for clarity
data['system'] = data['system'].replace({
    'xdbc-skip0': 'xdbc',
    'xdbc-skip1': 'xdbc[skip-ser]',
    'read_csv_url': 'netcat'
})

# Calculate the average time for each combination of table and system
average_times = (
    data.groupby(['table', 'system'])['time']
    .min()
    .reset_index()
    .pivot(index='table', columns='system', values='time')
)

# Define the order of systems and datasets for consistency
systems = ['xdbc', 'xdbc[skip-ser]', 'netcat']
tables = ['lineitem', 'acs', 'iot', 'icu']

# Reindex to ensure proper order and fill missing values with 0 (if any)
average_times = average_times.reindex(index=tables, columns=systems, fill_value=0)

# Extract data for plotting
approach_times = [average_times[system].values for system in systems]

# Set up plot style
plt.rcParams['text.usetex'] = True
plt.rcParams['font.family'] = 'serif'
plt.rcParams['font.serif'] = ['Computer Modern Roman']
plt.rcParams.update({'font.size': 16, 'axes.labelsize': 16, 'axes.titlesize': 16, 'legend.fontsize': 14})

# Create the plot
datasets = tables  # Adjust dataset names for readability
formal_palette = sns.color_palette("colorblind", len(systems))
bar_width = 0.25
x_indexes = np.arange(len(datasets))

plt.figure(figsize=(6, 3.75))

# Plotting each approach with offset for bar positions
for i, (system, times) in enumerate(zip(systems, approach_times)):
    plt.bar(x_indexes + (i - 1) * bar_width, times, width=bar_width, color=formal_palette[i], label=system, zorder=3)

# Labels and Title
plt.xlabel('Datasets')
plt.ylabel('Time (s)')
plt.xticks(ticks=x_indexes, labels=datasets)
plt.legend(loc='best')

# Grid for better readability
plt.grid(axis='y', alpha=0.3, zorder=0)

# Display the plot
plt.tight_layout()
plt.savefig('csv_netcat_env_local.pdf', bbox_inches='tight')
plt.show()


