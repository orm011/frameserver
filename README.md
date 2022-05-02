# naming frames.
Frames are named by their (keyframe_no, frame_no) position within the stream.
This makes it easy to get to keyframes and their successors but it is not always obvious what the predecessor of a frame is.
This makes it easy to work with a very basic index that can be built fast.

This naming will also match the naming one can derive from scanning the video linearly for feature extraction, eg in some batch process.

# frameserver
Convenience server to get frames from video for interactive visualization.  

It is meant to work on different containers (tested on .mov, .tsv, .mp4).
and also meant to work on many encodings, (as long as they are supported by pyav and the interface is similar),
but has only been tested on h264.

The first time a file is read, it will create an index (took around two seconds for a 1 hour videos in an HDD),
these indices are stored in .cache/frameserver. This latency is slightly larger than we would like for interactive use when 
the videos are large, but still acceptable. 

Once the index is created, latency will depend more on far from a keyframe the frame is. For long distances,
eg 150 frames after a keyframe, latency was observed to be around 700ms.

It is meant to work on long videos (eg one hour or more), It is definitely faster than using `ffmpeg -v select=eq(n,)`.


## installing
`pip install git+<>` should work.

## using
(choose your own port)
`uvicorn -m frameserver.server --port 8500`

`wget http://localhost:8500/frame/path/to/video.mp4?keyframe_no=12&frame_no=9`

  
