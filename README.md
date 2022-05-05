
# Frameserver
Convenience server to get frames from video for interactive visualization.  

It is meant to work on different extensions (tested on .mov, .tsv, .mp4) 
and also meant to work on many encodings, (as long as they are supported by `pyav`), but has only been tested on h264 streams.

The first time a file is read it will create an index. These indices are stored in ~/.cache/frameserver (or at FRAMESERVER_CACHE)

Index building latency: took around two seconds for a 1 hour video in an HDD (almost instant for shorter clips)

Once the index is created, latency will depend on how far from the last keyframe the frame is. For long distances,
eg 150 frames after a keyframe, latency was observed to be around 700ms.

It is meant to work well for accessing frames in long videos (eg one hour or more), unlike eg  `ffmpeg -v select=eq(n,)` and a lot of other 
libraries out there that i have tried, for example scikit-video.

# Naming frames.
Frames are named by their (keyframe_no, frame_no) position within the stream.

Pros:
This makes it easy to get to keyframes and their successors.
This naming makes it more transparent how expensive it will be to get to a frame (keyframes are fast)
This makes it easy to work with a very basic index that can be built fast.

Cons:
 It is not always obvious what the predecessor of a frame is.

This naming will also match the naming one can derive from scanning the video linearly for feature extraction, eg in some batch process.

## Installing
`pip install git+<>` should work.

## Using
(choose your own port)
`uvicorn -m frameserver.server --port 8500`

`wget http://localhost:8500/frame/path/to/video.mp4?keyframe_no=12&frame_no=9`
  
