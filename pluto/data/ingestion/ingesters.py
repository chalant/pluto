from os import path

import h5py
import numpy as np

from zipline.data.hdf5_daily_bars import (
	coerce_to_uint32,
	VERSION,
	DATA,
	INDEX,
	LIFETIMES,
	SCALING_FACTOR,
	DAY,
	SID,
	START_DATE,
	END_DATE,
	DEFAULT_SCALING_FACTORS
)


class DailyBarIngester(object):
	#todo: the ingester has a state: we have multiple behaviors for the write method
	# in the initial state is in write mode, then it is in append mode
	# if it is cleared, it goes into write mode.
	def __init__(self, file_path, date_chunk_size):
		self._file_path = file_path
		self._date_chunk_size = date_chunk_size

	def write(self, country_code, frames):
		#the client must call this method first for all country codes it requires
		'''

		Parameters
		----------
		country_code
		frames: iterable[tuple[str, pandas.DataFrame]]
		scaling_factors

		Notes
		-----
		We assume that all the frames share the same timestamp and sids

		'''

		scaling_factors = DEFAULT_SCALING_FACTORS

		with h5py.File(self._file_path, mode='a') as file:
			country_group = file.create_group(country_code)
			data_group = country_group.create_group(DATA)
			index_group = country_group.create_group(INDEX)
			lifetimes_group = country_group.create_group(LIFETIMES)

			chunks = None
			is_null_matrix = None

			for field, frame in frames:
				if not chunks:
					days, sids = frame.columns.values, frame.index.values
					index_group.create_dataset(
						SID,
						data=sids,
						maxshape=(None,))
					index_group.create_dataset(
						DAY,
						data=days.astype(np.int64),
						maxshape=(None,))

					ls = len(sids)
					if ls:
						chunks = (ls, min(self._date_chunk_size, len(days)))
					# initialize null matrix
					is_null_matrix = np.logical_and.reduce(
						[frame.T.isnull().values]
					)
				else:
					is_null_matrix = np.logical_and.reduce(
						[is_null_matrix,
						 frame.T.isnull().values]
					)

				frame.sort_index(inplace=True)
				frame.sort_index(axis='columns', inplace=True)

				#todo: how to fill data? => the client must provide "complete"
				# data, without "holes". holes are points where there is no data
				# for an active asset
				data = coerce_to_uint32(
					frame.values,
					scaling_factors[field]
				)

				dataset = data_group.create_dataset(
					field,
					compression='lzf',
					shuffle=True,
					data=data,
					maxshape=(None, None),
					chunks=chunks
				)

				dataset.attrs[SCALING_FACTOR] = scaling_factors[field]

			# Offset of the first null from the start of the input.
			start_date_ixs = is_null_matrix.argmin(axis=0)
			# Offset of the last null from the **end** of the input.
			end_offsets = is_null_matrix[::-1].argmin(axis=0)
			# Offset of the last null from the start of the input
			end_date_ixs = is_null_matrix.shape[0] - end_offsets - 1

			lifetimes_group.create_dataset(
				START_DATE,
				data=start_date_ixs,
				fill_value=1,
				maxshape=(None, None))

			lifetimes_group.create_dataset(
				END_DATE,
				data=end_date_ixs,
				fillvalue=1,
				maxshape=(None, None))

	def clear(self):
		pass

	def append(self, country_code, frames):
		#we assume that the data for all country codes has been initialized, else,
		# we throw an exception
		'''

		Parameters
		----------
		frames: iterable(tuple(str, pandas.DataFrame))

		'''
		file_path = self._file_path
		if path.exists(file_path):
			with h5py.File(file_path, mode='a') as file:
				#todo: what if the country_code group is empty? => we can't append
				# =>write
				data = file[country_code][DATA]
				is_null_matrix = None

				dx = None
				dy = None
				for field, frame in frames:
					#make sure the everything is sorted
					frame.sort_index(inplace=True)
					frame.sort_index(axis='columns', inplace=True)

					if not is_null_matrix:
						#update index
						index = file[INDEX]
						day = index[DAY]
						sid = index[SID]
						days, sids = frame.columns.values, frame.index.values
						sid.resize(frame.shape[0], axis=0)
						day.resize(day.shape[0] + frame.shape[1], axis=0)
						day[:] = days
						sid[:] = sids

						#initialize null matrix for computing lifetimes
						is_null_matrix = np.logical_and.reduce(
							[frame.T.isnull().values]
						)
					else:
						is_null_matrix = np.logical_and.reduce(
							[is_null_matrix, frame.T.isnull().values]
						)
					fl = data[field]
					flx, fly = fl.shape
					frx, fry = frame.shape
					#add columns for new data
					dy = fly + fry
					#means there is a new asset
					dx = frx
					fl.resize(dx, dy)
					#populate the last #fry columns of the dataset
					i = fry
					for column in frame:
						fl[:, dy-i] = frame[column].values
						i -= 1

				# Offset of the first null from the start of the input.
				start_date_ixs = is_null_matrix.argmin(axis=0)
				# Offset of the last null from the **end** of the input.
				end_offsets = is_null_matrix[::-1].argmin(axis=0)
				# Offset of the last null from the start of the input
				end_date_ixs = is_null_matrix.shape[0] - end_offsets - 1

				lifetimes = file[LIFETIMES]
				start_date = lifetimes[START_DATE]
				end_date = lifetimes[END_DATE]

				start_date.resize(dy, dx)
				end_date.resize(dy, dx)

				#add row of new lifetime indices
				start_date[:] = start_date_ixs
				end_date[:] = end_date_ixs
		else:
			raise FileNotFoundError('Must first write a dataset using write method')